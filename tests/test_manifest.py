"""M1: the manifest records every batch with its checksum and ingested_at."""

from __future__ import annotations

from datetime import date

import pytest

from collector import manifest as m
from collector import storage
from collector.manifest import BatchRecord, Manifest


@pytest.fixture
def man(tmp_path):
    with Manifest(tmp_path / "state" / "manifest.sqlite") as instance:
        yield instance


def record(**overrides) -> BatchRecord:
    defaults = {
        "batch_id": "eod-2026-09-30-r1",
        "dataset": "eod",
        "status": m.PUBLISHED,
        "source": "massive:grouped_daily",
        "params": {"date": "2026-09-30", "adjusted": False, "include_otc": False},
        "schema_version": 1,
        "ingested_at": m.utcnow(),
        "partition_day": date(2026, 9, 30),
        "path": "published/eod/year=2026/month=09/eod_2026-09.parquet",
        "rows": 10_412,
        "bytes": 845_221,
        "checksum_sha256": "a" * 64,
        "published_at": m.utcnow(),
        "checks": {"hard": "pass", "soft_flags": []},
        "run_id": "run-1",
    }
    return BatchRecord(**{**defaults, **overrides})


# --------------------------------------------------------------------------- #
# Batches
# --------------------------------------------------------------------------- #


def test_records_a_batch_with_checksum_and_ingested_at(man):
    batch = record()
    man.record_batch(batch)

    row = man.get_batch(batch.batch_id)
    assert row is not None
    assert row["dataset"] == "eod"
    assert row["status"] == m.PUBLISHED
    assert row["checksum_sha256"] == "a" * 64
    assert row["rows"] == 10_412
    assert row["schema_version"] == 1
    assert row["ingested_at"] == batch.ingested_at.isoformat()
    assert row["source"] == "massive:grouped_daily"


def test_params_are_stored_as_canonical_json(man):
    man.record_batch(record(params={"b": 2, "a": 1}))
    row = man.get_batch("eod-2026-09-30-r1")
    assert row["params_json"] == '{"a": 1, "b": 2}'  # sorted, so comparable


def test_recording_the_same_batch_id_twice_updates_rather_than_duplicates(man):
    man.record_batch(record(status=m.STAGED, checksum_sha256=None, published_at=None))
    man.record_batch(record(status=m.PUBLISHED))
    rows = list(man.conn.execute("SELECT * FROM batches"))
    assert len(rows) == 1
    assert rows[0]["status"] == m.PUBLISHED
    assert rows[0]["checksum_sha256"] == "a" * 64


def test_a_correction_is_a_new_revision_with_a_reason(man):
    """Published files are immutable; a correction appends history."""
    man.record_batch(record())
    assert man.next_revision("eod", date(2026, 9, 30)) == 2

    man.record_batch(
        record(
            batch_id="eod-2026-09-30-r2",
            revision=2,
            checksum_sha256="b" * 64,
            reason="provider restated volume for 3 tickers",
        )
    )
    latest = man.latest_published("eod", date(2026, 9, 30))
    assert latest["revision"] == 2
    assert latest["reason"].startswith("provider restated")
    # The earlier revision is still on record.
    assert len(list(man.conn.execute("SELECT 1 FROM batches"))) == 2


def test_quarantined_batches_are_not_reported_as_published(man):
    man.record_batch(record(status=m.QUARANTINED, reason="high < low on 2 rows"))
    assert man.published_batches("eod") == []
    assert man.latest_published("eod", date(2026, 9, 30)) is None


def test_next_revision_starts_at_one(man):
    assert man.next_revision("minute", date(2026, 9, 30)) == 1


# --------------------------------------------------------------------------- #
# Coverage: the self-healing mechanism
# --------------------------------------------------------------------------- #


def test_expected_days_become_gaps_until_they_succeed(man):
    days = [date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30)]
    man.expect("eod", days)
    assert [gap.day for gap in man.gaps("eod")] == days

    man.mark_coverage("eod", date(2026, 9, 29), m.OK, batch_id="b2")
    assert [gap.day for gap in man.gaps("eod")] == [date(2026, 9, 28), date(2026, 9, 30)]


def test_a_failed_day_is_retried_and_attempts_accumulate(man):
    man.expect("eod", [date(2026, 9, 30)])
    man.mark_coverage("eod", date(2026, 9, 30), m.FAILED, error="HTTP 503")
    man.mark_coverage("eod", date(2026, 9, 30), m.FAILED, error="HTTP 503")

    gaps = man.gaps("eod")
    assert len(gaps) == 1
    assert gaps[0].state == m.FAILED
    assert gaps[0].attempts == 2
    assert gaps[0].last_error == "HTTP 503"


def test_a_skipped_day_is_not_a_gap(man):
    """A non-trading day, or a delisted ticker, is explained -- not owed."""
    man.expect("eod", [date(2026, 9, 30)])
    man.mark_coverage("eod", date(2026, 9, 30), m.SKIPPED, error="market holiday")
    assert man.gaps("eod") == []


def test_expect_is_idempotent_and_does_not_reset_progress(man):
    man.expect("eod", [date(2026, 9, 30)])
    man.mark_coverage("eod", date(2026, 9, 30), m.OK)
    man.expect("eod", [date(2026, 9, 30), date(2026, 10, 1)])

    assert [gap.day for gap in man.gaps("eod")] == [date(2026, 10, 1)]
    assert man.coverage_summary("eod") == {m.OK: 1, m.PENDING: 1}


def test_per_ticker_coverage_for_minute_data(man):
    man.expect("minute", [date(2026, 9, 30)], tickers=["AAPL", "MSFT", "NVDA"])
    assert len(man.gaps("minute")) == 3

    man.mark_coverage("minute", date(2026, 9, 30), m.OK, ticker="AAPL")
    remaining = {gap.ticker for gap in man.gaps("minute")}
    assert remaining == {"MSFT", "NVDA"}


def test_gaps_can_be_limited_for_a_bounded_run(man):
    man.expect("minute", [date(2026, 9, 30)], tickers=[f"T{i:04d}" for i in range(50)])
    assert len(man.gaps("minute", limit=10)) == 10


def test_gaps_are_ordered_oldest_first(man):
    man.expect("eod", [date(2026, 9, 30), date(2026, 9, 1), date(2026, 9, 15)])
    assert [gap.day for gap in man.gaps("eod")] == [
        date(2026, 9, 1),
        date(2026, 9, 15),
        date(2026, 9, 30),
    ]


def test_coverage_is_scoped_per_dataset(man):
    man.expect("eod", [date(2026, 9, 30)])
    man.expect("minute", [date(2026, 9, 30)], tickers=["AAPL"])
    man.mark_coverage("eod", date(2026, 9, 30), m.OK)
    assert man.gaps("eod") == []
    assert len(man.gaps("minute")) == 1


# --------------------------------------------------------------------------- #
# Runs
# --------------------------------------------------------------------------- #


def test_runs_are_recorded_with_an_outcome(man):
    man.start_run("run-1", "eod")
    man.finish_run("run-1", "ok", {"published": 1, "rows": 10_412})

    runs = man.recent_runs()
    assert len(runs) == 1
    assert runs[0]["job"] == "eod"
    assert runs[0]["outcome"] == "ok"
    assert runs[0]["ended_at"] is not None
    assert '"rows": 10412' in runs[0]["detail_json"]


# --------------------------------------------------------------------------- #
# Integrity and migrations
# --------------------------------------------------------------------------- #


def test_verify_checksums_detects_tampering(tmp_path, eod_table):
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    staged = storage.stage(
        eod_table, dataset="eod", paths=paths, batch_id="b1", filename="eod_2026-09.parquet"
    )
    published = storage.publish(staged, paths.eod_path(date(2026, 9, 30)))

    with Manifest(paths.manifest_db) as man:
        man.record_batch(
            record(
                path=str(published.relative_to(paths.root)),
                checksum_sha256=staged.checksum_sha256,
                rows=staged.rows,
                bytes=staged.bytes,
            )
        )
        assert man.verify_checksums(paths.root) == []

        published.write_bytes(published.read_bytes() + b"tampered")
        problems = man.verify_checksums(paths.root)
        assert len(problems) == 1
        assert "checksum" in problems[0][1]


def test_verify_checksums_detects_a_missing_file(tmp_path):
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    with Manifest(paths.manifest_db) as man:
        man.record_batch(record(path="published/eod/year=2026/month=09/gone.parquet"))
        problems = man.verify_checksums(paths.root)
        assert len(problems) == 1
        assert "missing file" in problems[0][1]


def test_migrations_are_applied_once_and_reopening_is_safe(tmp_path):
    db = tmp_path / "manifest.sqlite"
    with Manifest(db) as first:
        first.record_batch(record())
        version = first.conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == m.SCHEMA_VERSION

    with Manifest(db) as second:
        assert second.conn.execute("PRAGMA user_version").fetchone()[0] == version
        assert second.get_batch("eod-2026-09-30-r1") is not None


def test_manifest_creates_its_parent_directory(tmp_path):
    with Manifest(tmp_path / "deep" / "nested" / "manifest.sqlite") as man:
        assert man.get_batch("nope") is None
