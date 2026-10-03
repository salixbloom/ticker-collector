"""Data quality checks, and the gate between staging and published.

:func:`run` assembles the checks that apply to a batch and returns a
:class:`CheckReport`. :func:`validate_and_publish` is the gate itself: a report
with any hard failure sends the batch to ``quarantine/`` with the report beside
it and records that in the manifest; otherwise the batch is published and any
soft flags are recorded alongside it.

Nothing else in the collector decides publish-or-quarantine. Keeping that in one
place is what makes "validate before publish" an invariant rather than a habit
each job has to remember.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa

from .. import manifest as manifest_module
from .. import storage
from ..manifest import BatchRecord, Manifest
from ..storage import DataPaths, StagedBatch
from . import rules
from .base import CheckReport, Finding, Severity, hard, soft
from .trading_calendar import DEFAULT as DEFAULT_CALENDAR
from .trading_calendar import TradingCalendar


@dataclass
class CheckContext:
    """Everything the checks need beyond the table itself.

    All optional: a check whose context is absent is skipped rather than guessed
    at, so a first run with no history does not invent comparisons.
    """

    day: date
    calendar: TradingCalendar = DEFAULT_CALENDAR
    #: Trailing 20-day median EOD ticker count, for the completeness check.
    trailing_ticker_median: int | None = None
    eod_count_soft_tolerance: float = 0.10
    eod_count_hard_tolerance: float = 0.30
    #: Previous trading day's closes, by ticker, for the large-move check.
    previous_closes: dict[str, int] = field(default_factory=dict)
    #: Tickers with a split executing on ``day``, which explains a large move.
    split_tickers: set[str] = field(default_factory=set)
    #: Split ratios by ticker, for the split-without-discontinuity check.
    split_ratios: dict[str, float] = field(default_factory=dict)
    #: The minute universe, for the coverage check.
    universe: list[str] = field(default_factory=list)
    #: Tickers legitimately absent, with the reason (halted, delisted).
    explained_absences: dict[str, str] = field(default_factory=dict)
    #: Count of values rounded per column during conversion.
    rounded_by_column: dict[str, int] = field(default_factory=dict)
    #: The published EOD table for ``day``, for minute reconciliation.
    eod_table: pa.Table | None = None


#: Datasets whose batch date is a trading session, and so must be one. The rest
#: are dated by the calendar day they were collected on.
SESSION_DATED = ("eod", "minute", "tickers", "universe")


def run(table: pa.Table, dataset: str, batch_id: str, context: CheckContext) -> CheckReport:
    """Run every check that applies to this batch."""
    report = CheckReport(dataset=dataset, batch_id=batch_id)

    # Structural. The schema, nullability and read-back checks already ran in
    # storage.stage(); these are the ones that need the dataset's semantics.
    # Splits and dividends are cumulative tables dated by *when they were
    # fetched*, not by a session -- the rows carry their own execution and
    # ex-dividend dates, as the comment below says. Checking the batch date
    # against the calendar made a Saturday fetch of perfectly good data a hard
    # failure, which on the deployed container meant corporate_actions failing
    # every weekend and taking container health down with it.
    if dataset in SESSION_DATED:
        _record(report, "is_trading_day", rules.check_is_trading_day(context.day, context.calendar))
    _record(report, "no_duplicate_keys", rules.check_no_duplicate_keys(table, dataset))

    # Only the bar datasets carry a per-row timestamp to bound. A ticker snapshot
    # or a corporate-actions table is dated by `snapshot_date`, and its rows refer
    # to dates (a split's execution date) that are deliberately not the batch's.
    if dataset in ("eod", "minute"):
        _record(
            report,
            "timestamps_in_window",
            rules.check_timestamps_in_window(table, dataset, context.day, context.calendar),
        )
    if dataset in ("tickers", "universe"):
        _record(
            report,
            "snapshot_date",
            rules.check_snapshot_date(table, context.day),
        )

    # Value sanity.
    _record(report, "prices_positive", rules.check_prices_positive(table, dataset))
    _record(report, "volume_non_negative", rules.check_volume_non_negative(table, dataset))
    _record(report, "ohlc_ordering", rules.check_ohlc_ordering(table, dataset))
    _record(report, "vwap_in_range", rules.check_vwap_in_range(table, dataset))
    _record(report, "precision_loss", rules.check_precision_loss(context.rounded_by_column))

    if dataset == "eod":
        _record(
            report,
            "eod_ticker_count",
            rules.check_eod_ticker_count(
                table,
                trailing_median=context.trailing_ticker_median,
                soft_tolerance=context.eod_count_soft_tolerance,
                hard_tolerance=context.eod_count_hard_tolerance,
            ),
        )
        _record(
            report,
            "large_moves",
            rules.check_large_moves(
                table,
                previous_closes=context.previous_closes,
                split_tickers=context.split_tickers,
            ),
        )
        if context.split_ratios:
            closes = dict(
                zip(
                    table.column("ticker").to_pylist(),
                    table.column("close").to_pylist(),
                    strict=True,
                )
            )
            _record(
                report,
                "split_discontinuity",
                rules.check_split_without_discontinuity(
                    splits=context.split_ratios,
                    previous_closes=context.previous_closes,
                    closes=closes,
                ),
            )

    if dataset == "tickers":
        _record_many(report, "ticker_snapshot_sanity", rules.check_ticker_snapshot_sanity(table))

    if dataset == "splits":
        _record(report, "split_ratios_sane", rules.check_split_ratios_sane(table))

    if dataset == "dividends":
        _record(report, "dividend_amounts_sane", rules.check_dividend_amounts_sane(table))

    if dataset == "minute":
        _record(
            report,
            "session_labels",
            rules.check_session_labels(table, context.day, context.calendar),
        )
        _record_many(
            report,
            "minute_bar_counts",
            rules.check_minute_bar_counts(table, context.day, context.calendar),
        )
        _record(
            report,
            "universe_coverage",
            rules.check_universe_coverage(
                table, universe=context.universe, explained=context.explained_absences
            ),
        )
        if context.eod_table is not None:
            _record_many(
                report,
                "minute_eod_reconciliation",
                rules.check_minute_eod_reconciliation(table, context.eod_table, day=context.day),
            )

    return report


def _record(report: CheckReport, name: str, finding: Finding | None) -> None:
    if finding is None:
        report.record_pass(name)
    else:
        report.add(finding)


def _record_many(report: CheckReport, name: str, findings: list[Finding]) -> None:
    if not findings:
        report.record_pass(name)
    else:
        report.extend(findings)


# --------------------------------------------------------------------------- #
# The gate
# --------------------------------------------------------------------------- #


@dataclass
class PublishResult:
    """What happened to one batch."""

    report: CheckReport
    published_path: Any | None = None
    quarantine_path: Any | None = None
    #: Rows in the batch itself (not the merged artifact).
    rows: int = 0

    @property
    def published(self) -> bool:
        return self.published_path is not None


def validate_and_publish(
    table: pa.Table,
    *,
    dataset: str,
    batch_id: str,
    paths: DataPaths,
    manifest: Manifest,
    destination: Any,
    context: CheckContext,
    source: str,
    params: dict[str, Any],
    filename: str | None = None,
    compression_level: int = 9,
    row_group_bytes: int = 134_217_728,
    run_id: str | None = None,
    merge: bool = False,
    track_coverage: bool = True,
) -> PublishResult:
    """Stage, check, then publish or quarantine. The only path into ``published/``.

    Order matters: the table is staged first so that the bytes being checked are
    the bytes that will be published, and so a failed batch still has a file to
    quarantine for diagnosis.

    ``merge=True`` publishes ``table`` merged into whatever is already at
    ``destination`` -- how a day is added to a monthly EOD file. The checks still
    run on the day alone; see :func:`_stage_merged`.

    ``track_coverage=False`` leaves the coverage table alone, for datasets whose
    work is tracked per ticker rather than per day. Minute bars are that case: the
    job owns ~1,000 ``(day, ticker)`` rows, and a day-level ``ticker=""`` row
    written here would be a phantom -- counted in the summary, and if the day were
    ever quarantined it would be marked failed with nothing able to clear it,
    wedging that day permanently.
    """
    staged: StagedBatch = storage.stage(
        table,
        dataset=dataset,
        paths=paths,
        batch_id=batch_id,
        filename=filename or f"{dataset}_{context.day.isoformat()}.parquet",
        compression_level=compression_level,
        row_group_bytes=row_group_bytes,
    )

    normalised = storage.read_parquet(staged.path)
    report = run(normalised, dataset, batch_id, context)

    ingested_at = manifest_module.utcnow()
    revision = manifest.next_revision(dataset, context.day)
    base: dict[str, Any] = {
        "batch_id": batch_id,
        "dataset": dataset,
        "source": source,
        "params": params,
        "schema_version": staged.schema_version,
        "ingested_at": ingested_at,
        "partition_day": context.day,
        "revision": revision,
        "rows": staged.rows,
        "source_rows": staged.rows,
        "bytes": staged.bytes,
        "checksum_sha256": staged.checksum_sha256,
        "content_sha256": staged.content_sha256,
        "checks": report.to_dict(),
        "run_id": run_id,
    }

    if not report.ok:
        quarantine_path = storage.quarantine(
            staged, paths=paths, batch_id=batch_id, report=report.to_json()
        )
        manifest.record_batch(
            BatchRecord(
                **base,
                status=manifest_module.QUARANTINED,
                path=str(quarantine_path),
                reason=report.summary(),
            )
        )
        if track_coverage:
            manifest.mark_coverage(
                dataset,
                context.day,
                manifest_module.FAILED,
                batch_id=batch_id,
                error=report.summary(),
            )
        return PublishResult(report=report, quarantine_path=quarantine_path, rows=staged.rows)

    artifact = staged
    if merge:
        artifact = _stage_merged(
            day_table=normalised,
            dataset=dataset,
            paths=paths,
            batch_id=batch_id,
            destination=destination,
            report=report,
            filename=filename or f"{dataset}_{context.day.isoformat()}.parquet",
            compression_level=compression_level,
            row_group_bytes=row_group_bytes,
        )
        # The merged artifact's size and file checksum describe the file that is
        # about to be written. `content_sha256` deliberately keeps describing the
        # *batch's own rows*, not the merged month: that is what lets verification
        # check each day independently inside a shared file, and what distinguishes
        # a provider restatement of one day from an ordinary append of the next.
        base["rows"] = artifact.rows
        base["bytes"] = artifact.bytes
        base["checksum_sha256"] = artifact.checksum_sha256
        base["checks"] = report.to_dict()
        if not report.ok:
            quarantine_path = storage.quarantine(
                artifact, paths=paths, batch_id=batch_id, report=report.to_json()
            )
            manifest.record_batch(
                BatchRecord(
                    **base,
                    status=manifest_module.QUARANTINED,
                    path=str(quarantine_path),
                    reason=report.summary(),
                )
            )
            if track_coverage:
                manifest.mark_coverage(
                    dataset,
                    context.day,
                    manifest_module.FAILED,
                    batch_id=batch_id,
                    error=report.summary(),
                )
            return PublishResult(report=report, quarantine_path=quarantine_path, rows=staged.rows)
        storage.clear_staging(paths, keep=[batch_id])

    published_path = storage.publish(artifact, destination)
    manifest.record_batch(
        BatchRecord(
            **base,
            status=manifest_module.PUBLISHED,
            path=str(published_path.relative_to(paths.root)),
            published_at=manifest_module.utcnow(),
            reason=report.summary() if report.soft_flags else None,
        )
    )
    if track_coverage:
        manifest.mark_coverage(dataset, context.day, manifest_module.OK, batch_id=batch_id)
    return PublishResult(report=report, published_path=published_path, rows=staged.rows)


def _stage_merged(
    *,
    day_table: pa.Table,
    dataset: str,
    paths: DataPaths,
    batch_id: str,
    destination: Any,
    report: CheckReport,
    filename: str,
    compression_level: int,
    row_group_bytes: int,
) -> StagedBatch:
    """Merge a validated day into its existing published partition.

    EOD files are monthly but the checks are per-day, so the two cannot be the
    same table: ``timestamps_in_window`` requires every row to carry the batch's
    date, which a whole month does not. So the day's batch is what gets checked,
    and the month is what gets written -- then the structural checks that *do*
    apply to a whole file (schema, and no duplicate keys across the merge) are
    re-run on the merged result before it can be published.
    """
    existing = storage.read_parquet(destination) if Path(str(destination)).exists() else None
    merged = storage.merge_rows(existing, day_table, dataset)

    merged_staged = storage.stage(
        merged,
        dataset=dataset,
        paths=paths,
        batch_id=batch_id,
        filename=f"merged_{filename}",
        compression_level=compression_level,
        row_group_bytes=row_group_bytes,
    )
    reloaded = storage.read_parquet(merged_staged.path)
    _record(report, "merged_no_duplicate_keys", rules.check_no_duplicate_keys(reloaded, dataset))
    try:
        storage.assert_schema(reloaded, dataset)
        report.record_pass("merged_schema")
    except storage.SchemaMismatch as exc:
        report.add(hard("merged_schema", str(exc)))
    return merged_staged


__all__ = [
    "CheckContext",
    "CheckReport",
    "Finding",
    "PublishResult",
    "Severity",
    "TradingCalendar",
    "hard",
    "rules",
    "run",
    "soft",
    "validate_and_publish",
]
