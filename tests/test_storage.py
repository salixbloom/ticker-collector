"""M1: writing and reading round-trips exactly; publish is atomic."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from collector import schemas, storage
from collector.money import from_scaled, to_scaled
from collector.storage import SchemaMismatch, StorageError

from .conftest import INGESTED_AT, eod_rows


def stage_eod(table, paths, batch_id="batch-1"):
    return storage.stage(
        table, dataset="eod", paths=paths, batch_id=batch_id, filename="eod_2026-09.parquet"
    )


# --------------------------------------------------------------------------- #
# Round-tripping
# --------------------------------------------------------------------------- #


def test_eod_round_trips_exactly(paths, eod_table):
    staged = stage_eod(eod_table, paths)
    published = storage.publish(staged, paths.eod_path(date(2026, 9, 30)))

    reloaded = storage.read_parquet(published)
    expected = storage.normalise(eod_table, "eod")
    assert reloaded.equals(expected, check_metadata=False)
    assert reloaded.schema.names == list(schemas.EOD_SCHEMA.names)
    for field in schemas.EOD_SCHEMA:
        assert reloaded.schema.field(field.name).type == field.type
        assert reloaded.schema.field(field.name).nullable == field.nullable


def test_minute_round_trips_exactly(paths, minute_table):
    staged = storage.stage(
        minute_table,
        dataset="minute",
        paths=paths,
        batch_id="m1",
        filename="minute_2026-09-30.parquet",
    )
    published = storage.publish(staged, paths.minute_path(date(2026, 9, 30)))
    reloaded = storage.read_parquet(published)
    assert reloaded.equals(storage.normalise(minute_table, "minute"), check_metadata=False)
    assert reloaded.column("session").to_pylist() == [1] * minute_table.num_rows


def test_scaled_integer_prices_survive_the_round_trip(paths, eod_table):
    """The whole point of scaled integers: no float drift through storage."""
    staged = stage_eod(eod_table, paths)
    published = storage.publish(staged, paths.eod_path(date(2026, 9, 30)))
    reloaded = storage.read_parquet(published)

    by_ticker = {t: i for i, t in enumerate(reloaded.column("ticker").to_pylist())}
    row = by_ticker["ZZZZ"]
    assert from_scaled(reloaded.column("low")[row].as_py()) == Decimal("0.0049")
    assert from_scaled(reloaded.column("high")[row].as_py()) == Decimal("0.0075")
    assert reloaded.column("vwap")[row].as_py() is None  # nullable, and null here

    row = by_ticker["AAPL"]
    assert from_scaled(reloaded.column("open")[row].as_py()) == Decimal("254.1234")
    assert from_scaled(reloaded.column("high")[row].as_py()) == Decimal("256.9999")


def test_schema_version_is_stamped_into_the_file(paths, eod_table):
    staged = stage_eod(eod_table, paths)
    assert storage.read_schema_version(staged.path) == schemas.SCHEMA_VERSION


def test_empty_table_round_trips(paths):
    empty = schemas.empty_table("eod")
    staged = storage.stage(
        empty, dataset="eod", paths=paths, batch_id="e", filename="eod_2026-09.parquet"
    )
    assert staged.rows == 0
    published = storage.publish(staged, paths.eod_path(date(2026, 9, 30)))
    assert storage.read_parquet(published).num_rows == 0


# --------------------------------------------------------------------------- #
# Determinism / idempotency
# --------------------------------------------------------------------------- #


def test_same_input_produces_the_same_checksum(paths, eod_table):
    first = stage_eod(eod_table, paths, batch_id="a")
    second = stage_eod(eod_table, paths, batch_id="b")
    assert first.checksum_sha256 == second.checksum_sha256


def test_row_order_does_not_affect_the_checksum(paths, eod_table):
    """Re-running a job may return rows in a different order; the file must not differ."""
    reversed_table = eod_table.take(list(reversed(range(eod_table.num_rows))))
    assert stage_eod(eod_table, paths, "a").checksum_sha256 == (
        stage_eod(reversed_table, paths, "b").checksum_sha256
    )


def test_column_order_does_not_affect_the_checksum(paths, eod_table):
    shuffled = eod_table.select(
        [
            "volume",
            "ticker",
            "close",
            "date",
            "open",
            "high",
            "low",
            "vwap",
            "transactions",
            "ingested_at",
        ]
    )
    assert stage_eod(eod_table, paths, "a").checksum_sha256 == (
        stage_eod(shuffled, paths, "b").checksum_sha256
    )


def test_republish_restores_an_identical_file(paths, eod_table):
    """M4's "delete a day and re-run" guarantee, at the storage layer."""
    destination = paths.eod_path(date(2026, 9, 30))
    first = storage.publish(stage_eod(eod_table, paths, "a"), destination)
    original = storage.sha256_file(first)

    destination.unlink()
    second = storage.publish(stage_eod(eod_table, paths, "b"), destination)
    assert storage.sha256_file(second) == original


# --------------------------------------------------------------------------- #
# Atomic publish
# --------------------------------------------------------------------------- #


def test_nothing_is_written_to_published_before_publish(paths, eod_table):
    stage_eod(eod_table, paths)
    assert list(paths.iter_published()) == []


def test_crash_mid_write_leaves_no_partial_file_in_published(paths, eod_table, monkeypatch):
    """Simulate the writer dying part-way through a batch."""
    real_write = storage.write_parquet
    calls: list[int] = []

    def exploding_write(table, path, **kwargs):
        calls.append(1)
        # Write some bytes, then die -- the worst case for a naive writer.
        from pathlib import Path

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(b"PAR1" + b"\x00" * 512)
        raise OSError("simulated crash mid-write")

    monkeypatch.setattr(storage, "write_parquet", exploding_write)
    with pytest.raises(OSError, match="simulated crash"):
        stage_eod(eod_table, paths)

    assert calls == [1]
    assert list(paths.iter_published()) == []
    assert not paths.eod_path(date(2026, 9, 30)).exists()

    # And the collector recovers: staging is cleared, then a real write succeeds.
    monkeypatch.setattr(storage, "write_parquet", real_write)
    storage.clear_staging(paths)
    published = storage.publish(
        stage_eod(eod_table, paths, "retry"), paths.eod_path(date(2026, 9, 30))
    )
    assert storage.read_parquet(published).num_rows == eod_table.num_rows


def test_a_corrupt_write_is_caught_by_readback_and_never_published(paths, eod_table, monkeypatch):
    def truncating_write(table, path, **kwargs):
        from pathlib import Path

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(b"not a parquet file at all")

    monkeypatch.setattr(storage, "write_parquet", truncating_write)
    with pytest.raises(Exception):  # noqa: B017 -- pyarrow raises its own error type
        stage_eod(eod_table, paths)
    assert list(paths.iter_published()) == []


def test_readback_mismatch_is_rejected(paths, eod_table, monkeypatch):
    """A writer that silently alters data must not get past staging."""
    original = storage.read_parquet

    def lying_read(path):
        table = original(path)
        return table.slice(0, max(table.num_rows - 1, 0))

    monkeypatch.setattr(storage, "read_parquet", lying_read)
    with pytest.raises(StorageError, match="did not read back identically"):
        stage_eod(eod_table, paths)
    assert list(paths.iter_published()) == []


def test_publish_replaces_an_existing_file_atomically(paths, eod_table):
    destination = paths.eod_path(date(2026, 9, 30))
    storage.publish(stage_eod(eod_table, paths, "a"), destination)

    later = pa.table(
        eod_rows(ingested_at=datetime(2026, 10, 1, 0, 0, tzinfo=UTC)),
        schema=schemas.EOD_SCHEMA,
    )
    storage.publish(stage_eod(later, paths, "b"), destination)

    reloaded = storage.read_parquet(destination)
    assert reloaded.num_rows == eod_table.num_rows  # replaced, not appended
    assert set(reloaded.column("ingested_at").to_pylist()) == {
        datetime(2026, 10, 1, 0, 0, tzinfo=UTC)
    }


def test_staging_directory_is_cleaned_up_after_publish(paths, eod_table):
    staged = stage_eod(eod_table, paths, "batch-xyz")
    storage.publish(staged, paths.eod_path(date(2026, 9, 30)))
    assert not (paths.staging / "batch-xyz").exists()


def test_clear_staging_removes_interrupted_batches(paths, eod_table):
    stage_eod(eod_table, paths, "interrupted-1")
    stage_eod(eod_table, paths, "interrupted-2")
    assert storage.clear_staging(paths) == 2
    assert list(paths.staging.iterdir()) == []


# --------------------------------------------------------------------------- #
# Quarantine
# --------------------------------------------------------------------------- #


def test_quarantine_keeps_the_file_and_its_report(paths, eod_table):
    staged = stage_eod(eod_table, paths, "bad-batch")
    target = storage.quarantine(
        staged, paths=paths, batch_id="bad-batch", report='{"check": "high<low", "rows": 3}'
    )
    assert target.exists()
    assert (paths.quarantine / "bad-batch" / "check_report.json").exists()
    assert list(paths.iter_published()) == []
    assert not (paths.staging / "bad-batch").exists()


# --------------------------------------------------------------------------- #
# Schema enforcement
# --------------------------------------------------------------------------- #


def test_missing_column_is_rejected(paths, eod_table):
    with pytest.raises(SchemaMismatch, match="missing columns"):
        storage.normalise(eod_table.drop_columns(["vwap"]), "eod")


def test_unexpected_column_is_rejected(eod_table):
    extra = eod_table.append_column("surprise", pa.array([1] * eod_table.num_rows))
    with pytest.raises(SchemaMismatch, match="unexpected columns"):
        storage.normalise(extra, "eod")


def test_float_prices_are_rejected_not_silently_cast(eod_table):
    """float32 loses precision on higher-priced stocks, so it must never appear."""
    floats = eod_table.set_column(
        eod_table.schema.get_field_index("close"),
        "close",
        pa.array([254.5, 410.25, 0.006], type=pa.float32()),
    )
    with pytest.raises(SchemaMismatch):
        storage.assert_schema(floats, "eod")


def test_null_in_a_required_column_is_rejected(eod_table):
    nulled = eod_table.set_column(
        eod_table.schema.get_field_index("close"),
        pa.field("close", pa.int64(), nullable=False),
        pa.array([1, None, 3], type=pa.int64()),
    )
    with pytest.raises(SchemaMismatch, match="null"):
        storage.assert_schema(nulled, "eod")


def test_nullability_difference_is_rejected(eod_table):
    relaxed_schema = pa.schema(
        [
            pa.field(f.name, f.type, nullable=True) if f.name == "close" else f
            for f in schemas.EOD_SCHEMA
        ]
    )
    relaxed = eod_table.cast(relaxed_schema)
    with pytest.raises(SchemaMismatch, match="nullable"):
        storage.assert_schema(relaxed, "eod")


def test_unknown_dataset_is_rejected():
    with pytest.raises(KeyError):
        schemas.get("fundamentals")


# --------------------------------------------------------------------------- #
# Layout
# --------------------------------------------------------------------------- #


def test_eod_is_partitioned_monthly(paths):
    first = paths.eod_path(date(2026, 9, 1))
    last = paths.eod_path(date(2026, 9, 30))
    assert first == last
    assert first.relative_to(paths.published).as_posix() == (
        "eod/year=2026/month=09/eod_2026-09.parquet"
    )


def test_minute_is_partitioned_daily(paths):
    path = paths.minute_path(date(2026, 9, 30))
    assert path.relative_to(paths.published).as_posix() == (
        "minute/year=2026/month=09/day=30/minute_2026-09-30.parquet"
    )
    assert paths.minute_path(date(2026, 9, 29)) != path


def test_reference_snapshots_are_dated(paths):
    assert paths.tickers_path(date(2026, 9, 30)).name == "tickers_2026-09-30.parquet"
    assert paths.universe_path(date(2026, 9, 30)).name == "universe_2026-09-30.parquet"


def test_date_partitioned_dataset_requires_a_day(paths):
    with pytest.raises(ValueError, match="day is required"):
        paths.published_path("minute")


def test_same_filesystem_check_passes_for_a_normal_layout(paths):
    paths.check_same_filesystem()  # must not raise


# --------------------------------------------------------------------------- #
# Merging (monthly EOD files)
# --------------------------------------------------------------------------- #


def test_merge_appends_a_new_day(eod_table):
    day_two = pa.table(eod_rows(day=date(2026, 10, 1)), schema=schemas.EOD_SCHEMA)
    merged = storage.merge_rows(eod_table, day_two, "eod")
    assert merged.num_rows == eod_table.num_rows * 2
    assert sorted(set(merged.column("date").to_pylist())) == [date(2026, 9, 30), date(2026, 10, 1)]


def test_merge_is_idempotent(eod_table):
    once = storage.merge_rows(eod_table, eod_table, "eod")
    twice = storage.merge_rows(once, eod_table, "eod")
    assert once.num_rows == eod_table.num_rows
    assert once.equals(twice)


def test_merge_lets_a_correction_replace_the_old_row(eod_table):
    corrected = eod_table.set_column(
        eod_table.schema.get_field_index("close"),
        "close",
        pa.array([to_scaled("999.0000")] * eod_table.num_rows, type=pa.int64()),
    )
    merged = storage.merge_rows(eod_table, corrected, "eod")
    assert merged.num_rows == eod_table.num_rows
    assert set(merged.column("close").to_pylist()) == {to_scaled("999.0000")}


def test_merge_into_nothing_returns_the_incoming_rows(eod_table):
    assert storage.merge_rows(None, eod_table, "eod").equals(storage.normalise(eod_table, "eod"))
    assert storage.merge_rows(schemas.empty_table("eod"), eod_table, "eod").num_rows == (
        eod_table.num_rows
    )


def test_merged_month_publishes_and_reads_back(paths, eod_table):
    destination = paths.eod_path(date(2026, 9, 30))
    storage.publish(stage_eod(eod_table, paths, "d1"), destination)

    day_two = pa.table(eod_rows(day=date(2026, 9, 29)), schema=schemas.EOD_SCHEMA)
    merged = storage.merge_rows(storage.read_parquet(destination), day_two, "eod")
    storage.publish(stage_eod(merged, paths, "d2"), destination)

    reloaded = storage.read_parquet(destination)
    assert reloaded.num_rows == eod_table.num_rows * 2
    # Sorted by ticker then date, per the dataset's sort order.
    assert reloaded.column("ticker").to_pylist() == ["AAPL", "AAPL", "MSFT", "MSFT", "ZZZZ", "ZZZZ"]
    assert reloaded.column("date").to_pylist()[:2] == [date(2026, 9, 29), date(2026, 9, 30)]


# --------------------------------------------------------------------------- #
# Compression
# --------------------------------------------------------------------------- #


def test_zstd_is_used_and_ticker_is_dictionary_encoded(paths, minute_table):
    import pyarrow.parquet as pq

    staged = storage.stage(
        minute_table, dataset="minute", paths=paths, batch_id="z", filename="m.parquet"
    )
    meta = pq.ParquetFile(staged.path).metadata
    group = meta.row_group(0)
    codecs = {group.column(i).compression for i in range(group.num_columns)}
    assert codecs == {"ZSTD"}

    ticker_col = next(
        group.column(i)
        for i in range(group.num_columns)
        if group.column(i).path_in_schema == "ticker"
    )
    assert "RLE_DICTIONARY" in ticker_col.encodings or "PLAIN_DICTIONARY" in ticker_col.encodings


def test_compression_level_is_honoured(paths, minute_table):
    sizes = {}
    for level in (1, 19):
        staged = storage.stage(
            minute_table,
            dataset="minute",
            paths=paths,
            batch_id=f"lvl{level}",
            filename="m.parquet",
            compression_level=level,
        )
        sizes[level] = staged.bytes
    # Both must be valid files; the sizes need not differ on a 10-row fixture, so
    # assert only that the level is accepted and the data survives.
    assert all(size > 0 for size in sizes.values())
    assert storage.read_parquet(paths.staging / "lvl19" / "m.parquet").num_rows == (
        minute_table.num_rows
    )


def test_ingested_at_is_preserved_to_the_millisecond(paths, eod_table):
    staged = stage_eod(eod_table, paths)
    reloaded = storage.read_parquet(staged.path)
    assert reloaded.column("ingested_at")[0].as_py() == INGESTED_AT
