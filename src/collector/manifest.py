"""The batch manifest: a SQLite record of every batch the collector touched.

The manifest is the collector's memory. Two things depend on it:

* **Self-healing.** Jobs do not ask "what is today?", they ask "which expected
  trading days are missing or failed?". That question is answered from the
  ``coverage`` table, so a missed day, outage or reboot repairs itself.
* **Point-in-time integrity.** Every batch records its source, parameters, row
  count, checksum, schema version, ``ingested_at`` and check results, so later
  phases can establish what was knowable when.

Published files are immutable. A correction is a *new* row with a higher
``revision`` for the same key, with its reason recorded -- history is appended,
never rewritten.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 4

#: Batch lifecycle states.
STAGED = "staged"
PUBLISHED = "published"
QUARANTINED = "quarantined"

#: Coverage states for an expected (dataset, day[, ticker]) unit of work.
PENDING = "pending"
OK = "ok"
FAILED = "failed"
SKIPPED = "skipped"  # legitimately absent: not a trading day, delisted, halted

_MIGRATIONS: tuple[str, ...] = (
    # v1 -- initial schema.
    """
    CREATE TABLE batches (
        batch_id        TEXT PRIMARY KEY,
        dataset         TEXT NOT NULL,
        status          TEXT NOT NULL,
        source          TEXT NOT NULL,
        params_json     TEXT NOT NULL,
        partition_day   TEXT,
        revision        INTEGER NOT NULL DEFAULT 1,
        path            TEXT,
        rows            INTEGER,
        bytes           INTEGER,
        checksum_sha256 TEXT,
        schema_version  INTEGER NOT NULL,
        ingested_at     TEXT NOT NULL,
        published_at    TEXT,
        check_json      TEXT,
        reason          TEXT,
        run_id          TEXT
    );
    CREATE INDEX idx_batches_dataset_day ON batches (dataset, partition_day);
    CREATE INDEX idx_batches_status      ON batches (status);
    CREATE INDEX idx_batches_run         ON batches (run_id);

    CREATE TABLE coverage (
        dataset      TEXT NOT NULL,
        day          TEXT NOT NULL,
        ticker       TEXT NOT NULL DEFAULT '',
        state        TEXT NOT NULL,
        attempts     INTEGER NOT NULL DEFAULT 0,
        last_batch   TEXT,
        last_error   TEXT,
        updated_at   TEXT NOT NULL,
        PRIMARY KEY (dataset, day, ticker)
    );
    CREATE INDEX idx_coverage_state ON coverage (dataset, state);

    CREATE TABLE runs (
        run_id      TEXT PRIMARY KEY,
        job         TEXT NOT NULL,
        started_at  TEXT NOT NULL,
        ended_at    TEXT,
        outcome     TEXT,
        detail_json TEXT
    );
    """,
    # v2 -- `rows` holds the published artifact's row count, which for a monthly
    # EOD file is the month to date. `source_rows` is what the provider returned
    # for this one batch, which is the only count comparable across days.
    """
    ALTER TABLE batches ADD COLUMN source_rows INTEGER;
    """,
    # v3 -- `checksum_sha256` covers the file, which includes `ingested_at` and so
    # changes on every re-download. `content_sha256` covers the data alone, so a
    # re-run of the same day matches and a provider restatement does not.
    """
    ALTER TABLE batches ADD COLUMN content_sha256 TEXT;
    CREATE INDEX idx_batches_content ON batches (dataset, content_sha256);
    """,
    # v4 -- small persistent facts the collector learns about the provider, the
    # first being the oldest date its plan will still serve. Learned rather than
    # configured, because the provider's history is a rolling window whose edge
    # moves forward every day.
    """
    CREATE TABLE settings (
        key        TEXT PRIMARY KEY,
        value      TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """,
)


def utcnow() -> datetime:
    """Current UTC time, millisecond precision to match the Arrow schemas."""
    now = datetime.now(UTC)
    return now.replace(microsecond=(now.microsecond // 1000) * 1000)


def _iso(value: datetime | date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


@dataclass(frozen=True)
class BatchRecord:
    """One manifest row."""

    batch_id: str
    dataset: str
    status: str
    source: str
    params: Mapping[str, Any]
    schema_version: int
    ingested_at: datetime
    partition_day: date | None = None
    revision: int = 1
    path: str | None = None
    rows: int | None = None
    #: Rows the provider returned for this batch (see migration v2).
    source_rows: int | None = None
    bytes: int | None = None
    checksum_sha256: str | None = None
    content_sha256: str | None = None
    published_at: datetime | None = None
    checks: Mapping[str, Any] | None = None
    reason: str | None = None
    run_id: str | None = None


@dataclass
class CoverageGap:
    """A unit of work a job still owes."""

    dataset: str
    day: date
    ticker: str = ""
    state: str = PENDING
    attempts: int = 0
    last_error: str | None = None


_COVERAGE_UPSERT = """
    INSERT INTO coverage (
        dataset, day, ticker, state, attempts, last_batch, last_error, updated_at
    ) VALUES (?,?,?,?,?,?,?,?)
    ON CONFLICT(dataset, day, ticker) DO UPDATE SET
        state=excluded.state,
        attempts=coverage.attempts + ?,
        last_batch=excluded.last_batch,
        last_error=excluded.last_error,
        updated_at=excluded.updated_at
"""


@dataclass(frozen=True)
class CoverageMark:
    """One unit's outcome, for writing a batch of them in one transaction."""

    day: date
    state: str
    ticker: str = ""
    batch_id: str | None = None
    error: str | None = None
    increment_attempt: bool = True


@dataclass
class Manifest:
    """Connection wrapper around ``state/manifest.sqlite``."""

    path: Path
    timeout: float = 30.0
    _conn: sqlite3.Connection | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    # -- connection ---------------------------------------------------------- #

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.path, timeout=self.timeout, isolation_level=None)
            self._conn.row_factory = sqlite3.Row
            # WAL keeps the daily jobs writing while the dashboard reads.
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._migrate(self._conn)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> Manifest:
        self.conn  # noqa: B018 -- open and migrate eagerly
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """Apply pending migrations, each in one transaction.

        ``BEGIN``/``COMMIT`` live inside the script rather than around it because
        ``executescript`` commits any open transaction before it runs, so wrapping
        the call would leave the statements uncovered. ``user_version`` is part of
        the database header and is transactional, so a half-applied migration
        cannot leave the version bumped.
        """
        current = conn.execute("PRAGMA user_version").fetchone()[0]
        for version in range(current, len(_MIGRATIONS)):
            script = (
                "BEGIN IMMEDIATE;\n"
                f"{_MIGRATIONS[version]}\n"
                f"PRAGMA user_version={version + 1};\n"
                "COMMIT;"
            )
            try:
                conn.executescript(script)
            except Exception:
                if conn.in_transaction:
                    conn.rollback()
                raise

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """A single write transaction. Retries briefly if the db is locked."""
        conn = self.conn
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                conn.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        try:
            yield conn
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    # -- batches ------------------------------------------------------------- #

    def record_batch(self, record: BatchRecord) -> None:
        """Insert or replace a batch row."""
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO batches (
                    batch_id, dataset, status, source, params_json, partition_day,
                    revision, path, rows, source_rows, bytes, checksum_sha256,
                    content_sha256, schema_version, ingested_at, published_at,
                    check_json, reason, run_id
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(batch_id) DO UPDATE SET
                    status=excluded.status,
                    path=excluded.path,
                    rows=excluded.rows,
                    source_rows=excluded.source_rows,
                    bytes=excluded.bytes,
                    checksum_sha256=excluded.checksum_sha256,
                    content_sha256=excluded.content_sha256,
                    published_at=excluded.published_at,
                    check_json=excluded.check_json,
                    reason=excluded.reason
                """,
                (
                    record.batch_id,
                    record.dataset,
                    record.status,
                    record.source,
                    json.dumps(dict(record.params), sort_keys=True, default=str),
                    _iso(record.partition_day),
                    record.revision,
                    record.path,
                    record.rows,
                    record.source_rows,
                    record.bytes,
                    record.checksum_sha256,
                    record.content_sha256,
                    record.schema_version,
                    _iso(record.ingested_at),
                    _iso(record.published_at),
                    None if record.checks is None else json.dumps(dict(record.checks), default=str),
                    record.reason,
                    record.run_id,
                ),
            )

    def get_batch(self, batch_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()

    def next_revision(self, dataset: str, partition_day: date | None) -> int:
        """The revision number a new batch for this partition should carry."""
        row = self.conn.execute(
            "SELECT MAX(revision) AS r FROM batches WHERE dataset = ? AND partition_day IS ?",
            (dataset, _iso(partition_day)),
        ).fetchone()
        return (row["r"] or 0) + 1

    def latest_published(self, dataset: str, partition_day: date | None) -> sqlite3.Row | None:
        """The current published batch for a partition, if any."""
        return self.conn.execute(
            """
            SELECT * FROM batches
             WHERE dataset = ? AND partition_day IS ? AND status = ?
             ORDER BY revision DESC LIMIT 1
            """,
            (dataset, _iso(partition_day), PUBLISHED),
        ).fetchone()

    def published_batches(self, dataset: str | None = None) -> list[sqlite3.Row]:
        if dataset is None:
            return list(
                self.conn.execute(
                    "SELECT * FROM batches WHERE status = ? ORDER BY dataset, partition_day",
                    (PUBLISHED,),
                )
            )
        return list(
            self.conn.execute(
                "SELECT * FROM batches WHERE status = ? AND dataset = ? ORDER BY partition_day",
                (PUBLISHED, dataset),
            )
        )

    # -- coverage ------------------------------------------------------------ #

    def expect(self, dataset: str, days: Sequence[date], tickers: Sequence[str] = ("",)) -> int:
        """Register units of work as expected. Existing rows keep their state.

        This is how a job learns what it owes: the schedule declares the expected
        trading days, and :meth:`gaps` then reports whatever is not yet ``ok``.
        """
        now = _iso(utcnow())
        rows = [
            (dataset, day.isoformat(), ticker, PENDING, now)
            for day in days
            for ticker in (tickers or ("",))
        ]
        if not rows:
            return 0
        with self.transaction() as conn:
            cursor = conn.executemany(
                """
                INSERT INTO coverage (dataset, day, ticker, state, updated_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(dataset, day, ticker) DO NOTHING
                """,
                rows,
            )
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    def mark_coverage(
        self,
        dataset: str,
        day: date,
        state: str,
        *,
        ticker: str = "",
        batch_id: str | None = None,
        error: str | None = None,
        increment_attempt: bool = True,
    ) -> None:
        """Set the state of one unit of work."""
        self.mark_coverage_many(
            dataset,
            [
                CoverageMark(
                    day=day,
                    state=state,
                    ticker=ticker,
                    batch_id=batch_id,
                    error=error,
                    increment_attempt=increment_attempt,
                )
            ],
        )

    def mark_coverage_many(self, dataset: str, marks: Sequence[CoverageMark]) -> None:
        """Set many units' states in ONE transaction.

        ``synchronous=FULL`` means every commit fsyncs, so marking a backfill
        chunk's ticker-days one at a time costs one fsync each -- 40 per API call
        at ``minute_backfill_chunk_days = 40``, ~48,000 per run at the deployed
        call budget, on an array-backed share where an fsync is not cheap.
        """
        if not marks:
            return
        now = _iso(utcnow())
        rows = [
            (
                dataset,
                mark.day.isoformat(),
                mark.ticker,
                mark.state,
                1 if mark.increment_attempt else 0,
                mark.batch_id,
                mark.error,
                now,
                1 if mark.increment_attempt else 0,
            )
            for mark in marks
        ]
        with self.transaction() as conn:
            conn.executemany(_COVERAGE_UPSERT, rows)

    def outstanding_by_day(self, dataset: str) -> dict[date, int]:
        """How many units each day still owes, in one query.

        The caller that needs this asks it for every day in the window, and the
        obvious spelling -- filter the full gap list per day -- is quadratic. At
        ~500 days against ~460,000 outstanding ticker-days that was 20 minutes of
        a run spent building dataclasses to throw away.
        """
        rows = self.conn.execute(
            """
            SELECT day, COUNT(*) AS n FROM coverage
             WHERE dataset = ? AND state IN (?, ?)
             GROUP BY day
            """,
            (dataset, PENDING, FAILED),
        )
        return {date.fromisoformat(row["day"]): row["n"] for row in rows}

    def gaps(
        self,
        dataset: str,
        *,
        states: Sequence[str] = (PENDING, FAILED),
        limit: int | None = None,
    ) -> list[CoverageGap]:
        """Units of work still owed, oldest first.

        This single query is the whole self-healing mechanism: whatever the reason
        a day is missing, it shows up here and the next run fills it.
        """
        placeholders = ",".join("?" for _ in states)
        sql = (
            f"SELECT * FROM coverage WHERE dataset = ? AND state IN ({placeholders}) "
            "ORDER BY day ASC, ticker ASC"
        )
        params: list[Any] = [dataset, *states]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return [
            CoverageGap(
                dataset=row["dataset"],
                day=date.fromisoformat(row["day"]),
                ticker=row["ticker"],
                state=row["state"],
                attempts=row["attempts"],
                last_error=row["last_error"],
            )
            for row in self.conn.execute(sql, params)
        ]

    def coverage_summary(self, dataset: str) -> dict[str, int]:
        return {
            row["state"]: row["n"]
            for row in self.conn.execute(
                "SELECT state, COUNT(*) AS n FROM coverage WHERE dataset = ? GROUP BY state",
                (dataset,),
            )
        }

    # -- settings ------------------------------------------------------------ #

    def get_setting(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def set_setting(self, key: str, value: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO settings (key, value, updated_at) VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value,
                                               updated_at=excluded.updated_at
                """,
                (key, value, _iso(utcnow())),
            )

    def delete_setting(self, key: str) -> None:
        with self.transaction() as conn:
            conn.execute("DELETE FROM settings WHERE key = ?", (key,))

    # -- the provider's rolling history window -------------------------------- #

    def history_floor(self, dataset: str) -> date | None:
        """Oldest date the provider will serve for ``dataset``, if known."""
        raw = self.get_setting(f"history_floor:{dataset}")
        if not raw:
            return None
        try:
            return date.fromisoformat(raw)
        except ValueError:
            return None

    def raise_history_floor(self, dataset: str, floor: date) -> bool:
        """Record that nothing before ``floor`` can be fetched. Returns True if raised.

        Only ever moves forward. The provider's window rolls, so the floor can
        rise but never fall -- and a lower value arriving later would mean asking
        again for dates already proven unavailable.
        """
        current = self.history_floor(dataset)
        if current is not None and floor <= current:
            return False
        self.set_setting(f"history_floor:{dataset}", floor.isoformat())
        return True

    def retire_before(self, dataset: str, floor: date, reason: str) -> int:
        """Mark every outstanding unit older than ``floor`` as skipped, in one write.

        One refusal tells us the whole date is outside the plan's window, for every
        ticker -- so learning it once and retiring the rest in a single statement is
        the difference between one API call and several hundred thousand.
        """
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE coverage
                   SET state = ?, last_error = ?, updated_at = ?
                 WHERE dataset = ? AND day < ? AND state IN (?, ?)
                """,
                (
                    SKIPPED,
                    reason,
                    _iso(utcnow()),
                    dataset,
                    floor.isoformat(),
                    PENDING,
                    FAILED,
                ),
            )
            return cursor.rowcount or 0

    # -- runs ---------------------------------------------------------------- #

    def start_run(self, run_id: str, job: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO runs (run_id, job, started_at) VALUES (?,?,?)",
                (run_id, job, _iso(utcnow())),
            )

    def finish_run(
        self, run_id: str, outcome: str, detail: Mapping[str, Any] | None = None
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE runs SET ended_at = ?, outcome = ?, detail_json = ? WHERE run_id = ?",
                (
                    _iso(utcnow()),
                    outcome,
                    None if detail is None else json.dumps(dict(detail), default=str),
                    run_id,
                ),
            )

    def recent_runs(self, limit: int = 20) -> list[sqlite3.Row]:
        return list(
            self.conn.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,))
        )

    # -- integrity ----------------------------------------------------------- #

    def verify_checksums(self, root: Path) -> list[tuple[str, str]]:
        """Re-verify every published batch against what is on disk.

        Two different questions, because a merged partition makes the naive one
        wrong. Several EOD batches share one monthly file, and each recorded the
        file's checksum *as it stood when that batch published*; adding the next
        day rewrites the file, so every earlier batch's file checksum stops
        matching. That is correct history, not corruption.

        So:

        * ``checksum_sha256`` (the whole file) is verified only for the newest
          batch at each path -- the one that actually wrote the current bytes.
        * ``content_sha256`` (the batch's own rows, ignoring ``ingested_at``) is
          verified for *every* batch, by reading the file and filtering to that
          batch's partition day. This is the stronger check: it confirms each
          day's data is still intact inside the merged file, and it catches a
          provider restatement, which a file-level hash cannot distinguish from
          an ordinary append.
        """
        from .storage import content_sha256, read_parquet, sha256_file

        problems: list[tuple[str, str]] = []
        rows = [row for row in self.published_batches() if row["path"]]

        def resolve(raw: str) -> Path:
            return Path(raw) if Path(raw).is_absolute() else root / raw

        # Newest batch per path owns the file's current bytes.
        newest: dict[str, sqlite3.Row] = {}
        for row in rows:
            current = newest.get(row["path"])
            if current is None or (row["revision"], row["ingested_at"]) > (
                current["revision"],
                current["ingested_at"],
            ):
                newest[row["path"]] = row

        for raw_path, row in newest.items():
            path = resolve(raw_path)
            if not path.exists():
                problems.append((row["batch_id"], f"missing file {path}"))
                continue
            actual = sha256_file(path)
            if row["checksum_sha256"] and actual != row["checksum_sha256"]:
                problems.append(
                    (
                        row["batch_id"],
                        f"file checksum {actual} != manifest {row['checksum_sha256']}",
                    )
                )

        # Per-batch content, read once per file.
        cache: dict[str, object] = {}
        for row in rows:
            if not row["content_sha256"] or not row["partition_day"]:
                continue
            path = resolve(row["path"])
            if not path.exists():
                if row["path"] not in newest or newest[row["path"]]["batch_id"] != row["batch_id"]:
                    problems.append((row["batch_id"], f"missing file {path}"))
                continue
            if row["path"] not in cache:
                try:
                    cache[row["path"]] = read_parquet(path)
                except Exception as exc:  # unreadable file is itself the problem
                    problems.append((row["batch_id"], f"unreadable {path}: {exc}"))
                    cache[row["path"]] = None
            table = cache[row["path"]]
            if table is None:
                continue

            day_rows = _rows_for_day(table, date.fromisoformat(row["partition_day"]))
            if day_rows is None:
                continue  # dataset has no per-day column; file hash already covered it
            if day_rows.num_rows == 0:
                problems.append((row["batch_id"], f"no rows for {row['partition_day']} in {path}"))
                continue
            actual = content_sha256(day_rows, row["dataset"])
            if actual != row["content_sha256"]:
                problems.append(
                    (
                        row["batch_id"],
                        f"content checksum {actual} != manifest {row['content_sha256']} "
                        f"(the provider's data for {row['partition_day']} has changed, "
                        "or the file was altered)",
                    )
                )
        return problems


def _rows_for_day(table, day: date):
    """Rows of ``table`` belonging to ``day``, or ``None`` if it has no day column.

    ``eod`` carries ``date``; ``minute`` carries ``ts`` and is partitioned one day
    per file, so the whole table is the day. Reference and corporate-action
    datasets have neither and are verified by file checksum alone.
    """
    import pyarrow.compute as pc

    if "date" in table.column_names:
        return table.filter(pc.equal(table.column("date"), day))
    if "ts" in table.column_names:
        return table
    return None
