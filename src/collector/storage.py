"""Parquet storage with validate-then-atomic-publish semantics.

Nothing is ever written directly into ``published/``. A batch is written to
``staging/``, read back and verified, then moved into place with ``os.replace``,
which is atomic on a single filesystem. A crash at any point before the replace
leaves ``published/`` untouched; a crash during the replace leaves either the old
file or the new one, never a partial one.

``staging/``, ``quarantine/`` and ``published/`` therefore have to live on the
same filesystem. :meth:`DataPaths.check_same_filesystem` asserts that at startup
so a misconfigured volume mount fails loudly instead of silently losing the
atomicity guarantee.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import money, schemas
from .schemas import Dataset

CHECKSUM_CHUNK_BYTES = 1 << 20


class StorageError(RuntimeError):
    """Raised when a write, verification or publish step fails."""


class SchemaMismatch(StorageError):
    """Raised when a table does not match its registered schema exactly."""


# --------------------------------------------------------------------------- #
# Layout
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class DataPaths:
    """The on-disk layout described in the plan, rooted at ``data_root``."""

    root: Path

    @classmethod
    def from_root(cls, root: str | os.PathLike[str]) -> DataPaths:
        return cls(Path(root))

    @property
    def published(self) -> Path:
        return self.root / "published"

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    @property
    def quarantine(self) -> Path:
        return self.root / "quarantine"

    @property
    def state(self) -> Path:
        return self.root / "state"

    @property
    def work(self) -> Path:
        """Resumable in-progress work, distinct from ``staging/``.

        ``staging/`` is cleared at startup because nothing in it matters once a
        run ends. Minute collection is different: one published day is assembled
        from ~1,000 separate API calls, so the parts fetched so far are real
        progress worth keeping across a restart. They live here instead.
        """
        return self.root / "work"

    def minute_part(self, day: date, ticker: str) -> Path:
        """Where one ticker's bars for one day wait until the day is complete."""
        return self.work / "minute" / day.isoformat() / f"{_safe_name(ticker)}.parquet"

    def minute_parts_dir(self, day: date) -> Path:
        return self.work / "minute" / day.isoformat()

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    @property
    def manifest_db(self) -> Path:
        return self.state / "manifest.sqlite"

    @property
    def status_file(self) -> Path:
        return self.state / "status.json"

    def ensure(self) -> DataPaths:
        """Create every directory the collector needs. Idempotent.

        Turns a bare ``PermissionError`` into something actionable: on Unraid the
        usual cause is a bind-mounted host directory owned by ``root`` while the
        container runs unprivileged, and the raw errno says nothing about that.
        """
        for path in (
            self.published,
            self.staging,
            self.quarantine,
            self.state,
            self.logs,
            self.work,
        ):
            try:
                path.mkdir(parents=True, exist_ok=True)
            except PermissionError as exc:
                raise StorageError(_permission_help(self.root, path, exc)) from exc
        return self

    def check_writable(self) -> None:
        """Confirm the collector can actually write to each directory it owns.

        A directory can exist and still be unwritable, which would otherwise only
        surface hours later when the first batch tried to publish.
        """
        self.ensure()
        for path in (
            self.published,
            self.staging,
            self.quarantine,
            self.state,
            self.logs,
            self.work,
        ):
            probe = path / ".write-probe"
            try:
                probe.write_bytes(b"")
                probe.unlink()
            except OSError as exc:
                raise StorageError(_permission_help(self.root, path, exc)) from exc

    def check_same_filesystem(self) -> None:
        """Verify the atomic-publish precondition."""
        self.ensure()
        devices = {
            name: path.stat().st_dev
            for name, path in (
                ("published", self.published),
                ("staging", self.staging),
                ("quarantine", self.quarantine),
            )
        }
        if len(set(devices.values())) > 1:
            raise StorageError(
                "published/, staging/ and quarantine/ must be on one filesystem for "
                f"atomic publish; got devices {devices}. Check the volume mounts."
            )

    # -- published paths, per dataset ---------------------------------------- #

    def eod_path(self, day: date) -> Path:
        """EOD is partitioned monthly; daily files would be tiny."""
        return (
            self.published
            / "eod"
            / f"year={day.year:04d}"
            / f"month={day.month:02d}"
            / f"eod_{day.year:04d}-{day.month:02d}.parquet"
        )

    def minute_path(self, day: date) -> Path:
        """Minute data is partitioned daily: one day is the unit of backfill."""
        return (
            self.published
            / "minute"
            / f"year={day.year:04d}"
            / f"month={day.month:02d}"
            / f"day={day.day:02d}"
            / f"minute_{day.isoformat()}.parquet"
        )

    def splits_path(self) -> Path:
        return self.published / "corporate_actions" / "splits.parquet"

    def dividends_path(self) -> Path:
        return self.published / "corporate_actions" / "dividends.parquet"

    def tickers_path(self, snapshot: date) -> Path:
        return self.published / "reference" / f"tickers_{snapshot.isoformat()}.parquet"

    def universe_path(self, snapshot: date) -> Path:
        return self.published / "reference" / f"universe_{snapshot.isoformat()}.parquet"

    def published_path(self, dataset: str, day: date | None = None) -> Path:
        """Dispatch to the right published path for a dataset."""
        if dataset == "eod":
            return self.eod_path(_require_day(dataset, day))
        if dataset == "minute":
            return self.minute_path(_require_day(dataset, day))
        if dataset == "splits":
            return self.splits_path()
        if dataset == "dividends":
            return self.dividends_path()
        if dataset == "tickers":
            return self.tickers_path(_require_day(dataset, day))
        if dataset == "universe":
            return self.universe_path(_require_day(dataset, day))
        raise KeyError(f"no published path for dataset {dataset!r}")

    def iter_published(self, dataset: str | None = None) -> Iterator[Path]:
        """Every published Parquet file, optionally for one dataset."""
        base = self.published / dataset if dataset else self.published
        if not base.exists():
            return
        yield from sorted(base.rglob("*.parquet"))


def _safe_name(ticker: str) -> str:
    """A filesystem-safe filename for a ticker.

    Real symbols contain dots, slashes and carets (``BRK.B``, ``ASGIrw``), so they
    cannot go straight into a path.
    """
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in ticker) or "_"


def _permission_help(root: Path, path: Path, exc: Exception) -> str:
    """An error an operator can act on, not just an errno.

    The overwhelmingly common cause on Unraid is a bind mount whose host
    directory is owned by root (Docker creates a missing bind source as root)
    while the image runs as an unprivileged user, so the message names that fix
    first and shows the ids actually in play.
    """
    try:
        uid, gid = os.getuid(), os.getgid()
        running_as = f"uid={uid} gid={gid}"
    except AttributeError:
        running_as = "this user"
    owner = ""
    try:
        existing = path if path.exists() else root
        if existing.exists():
            info = existing.stat()
            owner = f"; {existing} is owned by uid={info.st_uid} gid={info.st_gid}"
    except OSError:
        pass
    return (
        f"cannot write to {path}: {exc}. The collector runs as {running_as}{owner}. "
        f"On Unraid, make the host directory behind {root} writable by that user, e.g. "
        f"`chown -R 99:100 <host path>` for the usual nobody:users, or set APP_UID/"
        f"APP_GID at build time to match the share's owner."
    )


def _require_day(dataset: str, day: date | None) -> date:
    if day is None:
        raise ValueError(f"dataset {dataset!r} is partitioned by date; day is required")
    return day


# --------------------------------------------------------------------------- #
# Checksums
# --------------------------------------------------------------------------- #


def sha256_file(path: str | os.PathLike[str]) -> str:
    """SHA-256 of a file's bytes, streamed."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(CHECKSUM_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


#: Columns excluded from the content checksum: they record *when* we fetched, not
#: what the provider said.
CONTENT_EXCLUDED_COLUMNS = ("ingested_at",)


def content_sha256(table: pa.Table, dataset: Dataset | str) -> str:
    """Checksum of the data alone, ignoring ``ingested_at``.

    The plan asks for two things that cannot both hold literally: every batch
    records when it was downloaded, *and* re-running a day reproduces an identical
    file. A genuine re-download happens at a different time, so its
    ``ingested_at`` differs and so do the file's bytes.

    Faking the timestamp would be the wrong trade -- it is the point-in-time
    record the later phases depend on -- so the identity guarantee is expressed
    over the data instead. This checksum answers "is the provider still telling us
    the same thing for this day?", which is both what idempotency means here and
    what detects a silent provider restatement.
    """
    ds = schemas.get(dataset) if isinstance(dataset, str) else dataset
    normalised = normalise(table, ds)
    columns = [c for c in normalised.column_names if c not in CONTENT_EXCLUDED_COLUMNS]
    stripped = normalised.select(columns).replace_schema_metadata(None)

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, stripped.schema) as writer:
        writer.write_table(stripped)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


# --------------------------------------------------------------------------- #
# Normalisation and validation
# --------------------------------------------------------------------------- #


def normalise(table: pa.Table, dataset: Dataset | str) -> pa.Table:
    """Cast a table to its registered schema and sort it deterministically.

    Sorting is part of the storage contract, not a nicety: re-running a job for a
    date has to produce a byte-identical file, and that requires a total order on
    rows. The dataset's key columns are unique, so sorting by the sort columns and
    then by any remaining key columns gives a total order.
    """
    ds = schemas.get(dataset) if isinstance(dataset, str) else dataset
    missing = [name for name in ds.schema.names if name not in table.column_names]
    if missing:
        raise SchemaMismatch(f"{ds.name}: missing columns {missing}")
    extra = [name for name in table.column_names if name not in ds.schema.names]
    if extra:
        raise SchemaMismatch(f"{ds.name}: unexpected columns {extra}")

    ordered = table.select(list(ds.schema.names))
    try:
        cast = ordered.cast(ds.schema)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
        raise SchemaMismatch(f"{ds.name}: cannot cast to registered schema: {exc}") from exc

    order_by = list(ds.sort_by) + [c for c in ds.key if c not in ds.sort_by]
    if cast.num_rows:
        cast = cast.sort_by([(column, "ascending") for column in order_by])
    return cast.combine_chunks()


def assert_schema(table: pa.Table, dataset: Dataset | str) -> None:
    """Hard structural check: schema matches the registered version exactly."""
    ds = schemas.get(dataset) if isinstance(dataset, str) else dataset
    if table.schema.names != list(ds.schema.names):
        raise SchemaMismatch(
            f"{ds.name}: column names/order differ\n"
            f"  expected: {list(ds.schema.names)}\n  got:      {table.schema.names}"
        )
    for expected, actual in zip(ds.schema, table.schema, strict=True):
        if expected.type != actual.type:
            raise SchemaMismatch(
                f"{ds.name}.{expected.name}: type {actual.type} != expected {expected.type}"
            )
        if expected.nullable != actual.nullable:
            raise SchemaMismatch(
                f"{ds.name}.{expected.name}: nullable={actual.nullable} != "
                f"expected nullable={expected.nullable}"
            )
    for field in ds.schema:
        if not field.nullable and table.num_rows:
            nulls = table.column(field.name).null_count
            if nulls:
                raise SchemaMismatch(
                    f"{ds.name}.{field.name}: {nulls} null(s) in a non-nullable column"
                )


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #


def write_parquet(
    table: pa.Table,
    path: str | os.PathLike[str],
    *,
    dataset: Dataset | str,
    compression_level: int = 9,
    row_group_bytes: int = 134_217_728,
) -> None:
    """Write one Parquet file, fsynced, with the dataset's encoding settings."""
    ds = schemas.get(dataset) if isinstance(dataset, str) else dataset
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    meta = dict(table.schema.metadata or {})
    meta[schemas.META_SCHEMA_VERSION] = str(schemas.SCHEMA_VERSION).encode()
    meta[schemas.META_DATASET] = ds.name.encode()
    # The scale is stamped into every file: the int64 price and volume columns are
    # meaningless without it, and a reader should never have to guess or hardcode.
    meta[schemas.META_SCALE] = str(money.SCALE).encode()
    table = table.replace_schema_metadata(meta)

    row_group_size = _row_group_rows(table, row_group_bytes)
    with open(path, "wb") as handle:
        pq.write_table(
            table,
            handle,
            compression="zstd",
            compression_level=compression_level,
            use_dictionary=list(ds.dictionary_columns),
            write_statistics=True,
            row_group_size=row_group_size,
            version="2.6",
            store_schema=True,
        )
        handle.flush()
        os.fsync(handle.fileno())


def _row_group_rows(table: pa.Table, row_group_bytes: int) -> int | None:
    """Rows per row group that approximates the uncompressed byte target."""
    if table.num_rows == 0:
        return None
    bytes_per_row = max(table.nbytes / table.num_rows, 1.0)
    return max(int(row_group_bytes / bytes_per_row), 1)


def read_parquet(path: str | os.PathLike[str]) -> pa.Table:
    """Read a published or staged file back.

    ``ParquetFile.read`` rather than ``read_table``: the latter goes through the
    dataset layer, which infers Hive partition columns from the directory names.
    Our published paths are Hive-style (``year=2026/month=09/``), so that would
    silently add ``year`` and ``month`` columns and the file would no longer match
    its registered schema.
    """
    return pq.ParquetFile(path).read().combine_chunks()


def read_schema_version(path: str | os.PathLike[str]) -> int | None:
    """The ``schema_version`` stamped into a file's Parquet metadata."""
    return _read_int_meta(path, schemas.META_SCHEMA_VERSION)


def read_scale(path: str | os.PathLike[str]) -> int | None:
    """The price/volume scale stamped into a file's Parquet metadata."""
    return _read_int_meta(path, schemas.META_SCALE)


def _read_int_meta(path: str | os.PathLike[str], key: bytes) -> int | None:
    meta = pq.read_schema(path).metadata or {}
    raw = meta.get(key)
    return int(raw) if raw is not None else None


# --------------------------------------------------------------------------- #
# Staging and publishing
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class StagedBatch:
    """A validated file in ``staging/``, ready to publish or quarantine."""

    dataset: str
    path: Path
    rows: int
    bytes: int
    checksum_sha256: str
    #: Checksum of the data alone, ignoring ``ingested_at``. See content_sha256.
    content_sha256: str
    schema_version: int


def stage(
    table: pa.Table,
    *,
    dataset: Dataset | str,
    paths: DataPaths,
    batch_id: str,
    filename: str,
    compression_level: int = 9,
    row_group_bytes: int = 134_217_728,
    verify_readback: bool = True,
) -> StagedBatch:
    """Normalise, write to ``staging/`` and verify the file reads back exactly.

    Read-back verification is what makes the manifest checksum meaningful: a file
    whose bytes landed wrong, or whose types were silently widened by the writer,
    is caught here rather than discovered months later.
    """
    ds = schemas.get(dataset) if isinstance(dataset, str) else dataset
    normalised = normalise(table, ds)
    assert_schema(normalised, ds)

    staged_dir = paths.staging / batch_id
    staged_dir.mkdir(parents=True, exist_ok=True)
    staged_path = staged_dir / filename

    write_parquet(
        normalised,
        staged_path,
        dataset=ds,
        compression_level=compression_level,
        row_group_bytes=row_group_bytes,
    )

    if verify_readback:
        reloaded = read_parquet(staged_path)
        assert_schema(reloaded, ds)
        if not reloaded.equals(normalised, check_metadata=False):
            raise StorageError(
                f"{ds.name}: staged file {staged_path} did not read back identically"
            )
        version = read_schema_version(staged_path)
        if version != schemas.SCHEMA_VERSION:
            raise StorageError(
                f"{ds.name}: staged file schema_version {version} != {schemas.SCHEMA_VERSION}"
            )

    return StagedBatch(
        dataset=ds.name,
        path=staged_path,
        rows=normalised.num_rows,
        bytes=staged_path.stat().st_size,
        checksum_sha256=sha256_file(staged_path),
        content_sha256=content_sha256(normalised, ds),
        schema_version=schemas.SCHEMA_VERSION,
    )


def publish(staged: StagedBatch, destination: str | os.PathLike[str]) -> Path:
    """Atomically move a staged batch into ``published/``.

    ``os.replace`` is atomic within a filesystem, so readers see either the
    previous file or the new one. The parent directory is fsynced afterwards so
    the rename itself survives a power loss, not just the file contents.
    """
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staged.path, destination)
    _fsync_dir(destination.parent)

    actual = sha256_file(destination)
    if actual != staged.checksum_sha256:
        raise StorageError(
            f"checksum changed during publish of {destination}: "
            f"{staged.checksum_sha256} -> {actual}"
        )
    _cleanup_batch_dir(staged.path.parent)
    return destination


def quarantine(
    staged_or_path: StagedBatch | Path,
    *,
    paths: DataPaths,
    batch_id: str,
    report: str,
) -> Path:
    """Move a failed batch to ``quarantine/`` alongside its check report.

    Failed batches are kept, not deleted: the report plus the actual bytes are
    what make a failure diagnosable after the fact.
    """
    source = (
        staged_or_path.path if isinstance(staged_or_path, StagedBatch) else Path(staged_or_path)
    )
    target_dir = paths.quarantine / batch_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir
    if source.exists():
        target = target_dir / source.name
        os.replace(source, target)
    (target_dir / "check_report.json").write_text(report, encoding="utf-8")
    _fsync_dir(target_dir)
    if source.parent != target_dir:
        _cleanup_batch_dir(source.parent)
    return target


def _cleanup_batch_dir(directory: Path) -> None:
    """Remove an emptied staging batch directory; leave it if anything remains."""
    try:
        if directory.is_dir() and not any(directory.iterdir()):
            directory.rmdir()
    except OSError:
        pass


def _fsync_dir(directory: Path) -> None:
    """fsync a directory so a rename is durable. No-op where unsupported."""
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        # Windows cannot open a directory for reading; there is no portable
        # directory fsync there. Linux (the deployment target) can.
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def clear_staging(paths: DataPaths, *, keep: Sequence[str] = ()) -> int:
    """Drop leftover staging batches from an interrupted run.

    Staging holds nothing of value once a run ends -- anything that mattered was
    either published or quarantined -- so clearing it at startup keeps repeated
    crashes from accumulating junk.
    """
    if not paths.staging.exists():
        return 0
    kept = set(keep)
    removed = 0
    for child in paths.staging.iterdir():
        if child.name in kept:
            continue
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)
        removed += 1
    return removed


# --------------------------------------------------------------------------- #
# Merging into an existing published file
# --------------------------------------------------------------------------- #


def merge_rows(existing: pa.Table | None, incoming: pa.Table, dataset: Dataset | str) -> pa.Table:
    """Combine new rows into an existing file's rows, the incoming row winning.

    Used for the monthly EOD files, where adding a day means rewriting the month.
    Keeping the incoming row on a key collision makes a re-run a correction
    rather than a duplicate, which is what idempotency requires.
    """
    ds = schemas.get(dataset) if isinstance(dataset, str) else dataset
    incoming = normalise(incoming, ds)
    if existing is None or existing.num_rows == 0:
        return incoming
    existing = normalise(existing, ds)

    keys = _key_set(incoming, ds.key)
    mask = [key not in keys for key in _key_iter(existing, ds.key)]
    retained = existing.filter(pa.array(mask, type=pa.bool_()))
    return normalise(pa.concat_tables([retained, incoming]), ds)


def _key_iter(table: pa.Table, key: Sequence[str]) -> Iterator[tuple]:
    columns = [table.column(name).to_pylist() for name in key]
    return zip(*columns, strict=True)


def _key_set(table: pa.Table, key: Sequence[str]) -> set[tuple]:
    return set(_key_iter(table, key))


def temp_dir(paths: DataPaths, prefix: str = "tmp-") -> Path:
    """A scratch directory inside ``staging/`` (same filesystem as published)."""
    paths.staging.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=prefix, dir=paths.staging))
