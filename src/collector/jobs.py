"""The collection jobs.

Every job is a "fill what is missing" job. It asks the manifest which expected
trading days are not yet ``ok``, and fills those -- it never asks "what is
today?". That is the whole self-healing mechanism: a missed run, a provider
outage or a server reboot leaves the same gaps behind, and the next run closes
them without anyone intervening.

Consequences worth being explicit about:

* Running a job twice is harmless. The second run finds no gaps.
* A job interrupted half way resumes exactly where it stopped.
* The schedule only decides *when* catch-up happens, not what gets done.
"""

from __future__ import annotations

import logging
import shutil
import statistics
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa

from . import backup as backup_module
from . import checks, money, parse, schemas, storage
from . import manifest as manifest_module
from . import source as source_module
from . import status as status_module
from .checks import CheckContext
from .checks.trading_calendar import EXCHANGE_TZ, TradingCalendar
from .config import Config
from .manifest import Manifest
from .source import Client, NotAuthorizedError, PartialResponse, SourceError
from .status import JobStatus

log = logging.getLogger(__name__)

#: Trailing window for the EOD ticker-count comparison, per the plan.
TICKER_COUNT_WINDOW = 20

#: Datasets each job owns, for rolling coverage up into status.json. A job's name
#: is not always its dataset's name: `reference` writes `tickers`, and
#: `corporate_actions` writes two datasets.
JOB_DATASETS: dict[str, tuple[str, ...]] = {
    "eod": ("eod",),
    "minute": ("minute",),
    "reference": ("tickers",),
    "corporate_actions": ("splits", "dividends"),
    "universe": ("universe",),
    # Writes no dataset of its own; it only summarises the others.
    "checks_report": (),
    "backup": (),
}


@dataclass
class JobResult:
    """What one job run did."""

    job: str
    run_id: str
    published: int = 0
    quarantined: int = 0
    skipped: int = 0
    rows: int = 0
    soft_flags: list[str] = field(default_factory=list)
    #: Fetch and transport failures -- a day that never produced a batch.
    errors: list[str] = field(default_factory=list)
    #: Batches that were produced but failed a hard check.
    quarantine_reasons: list[str] = field(default_factory=list)
    aborted: bool = False

    @property
    def attempted(self) -> int:
        return self.published + self.failed

    @property
    def failed(self) -> int:
        """Days that did not publish. Counted once each, whatever went wrong."""
        return self.quarantined + len(self.errors)

    @property
    def outcome(self) -> str:
        if self.aborted:
            return "aborted"
        if self.failed:
            return "hard_failure"
        if self.soft_flags:
            return "soft_flags"
        return "ok"

    def to_status(self) -> JobStatus:
        return JobStatus(
            job=self.job,
            outcome=self.outcome,
            batches_published=self.published,
            batches_quarantined=self.quarantined,
            rows=self.rows,
            soft_flags=self.soft_flags[:20],
            errors=(self.errors + self.quarantine_reasons)[:20],
        )


@dataclass
class Context:
    """Everything a job needs, assembled once by the caller."""

    config: Config
    client: Client
    manifest: Manifest
    paths: storage.DataPaths
    calendar: TradingCalendar

    @property
    def compression_level(self) -> int:
        return self.config.storage.compression_level

    @property
    def row_group_bytes(self) -> int:
        return self.config.storage.row_group_bytes


# --------------------------------------------------------------------------- #
# The provider's rolling history window
# --------------------------------------------------------------------------- #

#: Reason recorded against units retired for being outside the plan's window.
AGED_OUT = "outside the provider's history window; this date can no longer be fetched"


def history_floor(ctx: Context, dataset: str, *, today: date) -> date:
    """The oldest date worth asking for, as currently understood.

    Two inputs. The **learned** floor comes from the provider actually refusing a
    date, and only ever moves forward. The **configured** floor is
    ``backfill_days`` minus a safety margin, which keeps the first run off the
    very edge of the window -- without it every run spends calls rediscovering
    that the oldest day has just aged out.
    """
    margin = ctx.config.collection.history_safety_days
    span = max(ctx.config.collection.backfill_days - margin, 1)
    configured = today - timedelta(days=span)
    learned = ctx.manifest.history_floor(dataset)
    return max(configured, learned) if learned else configured


#: Ticker used to probe the provider's window. Liquid and long-listed, so it has
#: data on every day the plan covers.
PROBE_TICKER = "AAPL"


def retire_aged_out(ctx: Context, dataset: str, *, today: date) -> int:
    """Retire units that slid out of the window while they were still owed.

    The provider's window is rolling, so the oldest date worth asking for moves
    forward every day. Both bar jobs bound their work by ``history_floor``, which
    means a unit left pending or failed below the new edge is never worked again
    -- but it stays in ``gaps()``, and so in the "still owed" figure the operator
    is meant to act on. During a long backfill that figure would drift further
    from the truth every day: ~1,000 minute ticker-days fall below the edge per
    trading day and would accumulate as permanent phantoms.

    ``note_unauthorized`` retires in the same way, but only when the provider
    actually refuses a date. This is the half that has no refusal to trigger it:
    the edge simply moved.
    """
    floor = history_floor(ctx, dataset, today=today)
    retired = ctx.manifest.retire_before(dataset, floor, AGED_OUT)
    if retired:
        log.info("%s: retired %d unit(s) that aged out below %s", dataset, retired, floor)
    return retired


def note_unauthorized(ctx: Context, dataset: str, day: date, *, today: date) -> int:
    """Learn the provider's real window edge, then retire everything below it.

    A refusal only tells us that *this* date is gone. Setting the floor to
    ``day + 1`` and walking on would then refuse the next date too, and the next:
    the floor would crawl forward one API call at a time, which is the problem it
    was meant to solve.

    So one refusal triggers a single probe that returns the actual boundary (see
    ``source.probe_history_floor``), and everything below it is retired in one
    write. Worst case that is two calls, not one per dead day.
    """
    floor = _discover_floor(ctx, today=today) or (day + timedelta(days=1))
    retired = 0
    # The window is a property of the plan, not of a dataset: measured on
    # 2026-10-02, minute and EOD refused and served exactly the same dates.
    for owner in ("eod", "minute"):
        ctx.manifest.raise_history_floor(owner, floor)
        retired += ctx.manifest.retire_before(owner, floor, AGED_OUT)
    log.warning(
        "%s: %s is outside the provider's history window. Its oldest available "
        "date is %s; retired %d outstanding unit(s) older than that. Data before "
        "that date can no longer be fetched from this plan.",
        dataset,
        day.isoformat(),
        floor.isoformat(),
        retired,
    )
    return retired


def _discover_floor(ctx: Context, *, today: date) -> date | None:
    """Ask the provider where its window starts. One call, cached by the caller."""
    oldest = today - timedelta(days=ctx.config.collection.backfill_days + 60)
    try:
        found = source_module.probe_history_floor(
            ctx.client, PROBE_TICKER, oldest, today - timedelta(days=1)
        )
    except (SourceError, PartialResponse) as exc:
        log.warning("could not probe the provider's history window: %s", exc)
        return None
    if found is not None:
        log.info("provider history window starts %s", found.isoformat())
    return found


# --------------------------------------------------------------------------- #
# EOD
# --------------------------------------------------------------------------- #


def expected_eod_days(ctx: Context, *, today: date, backfill_days: int | None = None) -> list[date]:
    """Trading days the collector should hold EOD data for.

    The window ends *yesterday*: today's bar does not exist until the session has
    closed and the provider has published it, so expecting it would manufacture a
    permanent gap.
    """
    # `is not None`, not `or`: an explicit 0 means "just yesterday", and `or`
    # would quietly substitute the configured 730-day window instead.
    days = backfill_days if backfill_days is not None else ctx.config.collection.backfill_days
    end = today - timedelta(days=1)
    start = end - timedelta(days=days)
    # Never expect a date the provider has already refused, or one inside the
    # safety margin at the edge of its rolling window.
    start = max(start, history_floor(ctx, "eod", today=today))
    return ctx.calendar.trading_days(start, end)


def run_eod(
    ctx: Context,
    *,
    today: date | None = None,
    backfill_days: int | None = None,
    max_days: int | None = None,
) -> JobResult:
    """Fill every missing EOD trading day.

    ``max_days`` bounds one invocation so the daily job cannot spend hours in a
    backfill; the rest is picked up by the next run, which is exactly the
    behaviour the self-healing design relies on.
    """
    today = today or date.today()
    run_id = f"eod-{uuid.uuid4().hex[:12]}"
    result = JobResult(job="eod", run_id=run_id)
    ctx.manifest.start_run(run_id, "eod")

    result.skipped += retire_aged_out(ctx, "eod", today=today)
    expected = expected_eod_days(ctx, today=today, backfill_days=backfill_days)
    ctx.manifest.expect("eod", expected)
    expected_set = set(expected)

    gaps = [g for g in ctx.manifest.gaps("eod") if g.day in expected_set]
    if max_days is not None:
        gaps = gaps[:max_days]
    log.info("eod: %d expected day(s), %d gap(s) to fill", len(expected), len(gaps))

    # Re-read before each fetch: a refusal mid-loop raises it, and the `gaps`
    # list was materialised before any of that happened. Without this check the
    # loop keeps calling the API for days it has just proven unavailable.
    floor = ctx.manifest.history_floor("eod")

    for gap in gaps:
        if floor is not None and gap.day < floor:
            ctx.manifest.mark_coverage(
                "eod",
                gap.day,
                manifest_module.SKIPPED,
                error=AGED_OUT,
                increment_attempt=False,
            )
            result.skipped += 1
            continue
        try:
            outcome = _collect_eod_day(ctx, gap.day, run_id=run_id)
        except NotAuthorizedError:
            # Permanent for this date and every earlier one. One probe finds the
            # real edge and the rest are retired in a single write.
            retired = note_unauthorized(ctx, "eod", gap.day, today=today)
            result.skipped += 1 + retired
            ctx.manifest.mark_coverage(
                "eod", gap.day, manifest_module.SKIPPED, error=AGED_OUT, increment_attempt=False
            )
            floor = ctx.manifest.history_floor("eod")
            continue
        except (SourceError, PartialResponse) as exc:
            message = f"{gap.day.isoformat()}: {exc}"
            log.warning("eod: %s", message)
            result.errors.append(message)
            ctx.manifest.mark_coverage("eod", gap.day, manifest_module.FAILED, error=str(exc)[:500])
        else:
            if outcome.published:
                result.published += 1
                result.rows += outcome.rows
                result.soft_flags.extend(
                    f"{gap.day.isoformat()}: {f.message}" for f in outcome.report.soft_flags
                )
            else:
                result.quarantined += 1
                result.quarantine_reasons.append(
                    f"{gap.day.isoformat()}: {outcome.report.summary()}"
                )

        abort = checks.rules.check_run_failure_ratio(
            failed=result.failed,
            total=result.attempted,
            abort_ratio=ctx.config.checks.run_failure_abort_ratio,
        )
        # Only abort once there is enough evidence to mean anything.
        if abort is not None and result.attempted >= 5:
            log.error("eod: %s", abort.message)
            result.aborted = True
            result.errors.append(abort.message)
            break

    ctx.manifest.finish_run(
        run_id,
        result.outcome,
        {
            "published": result.published,
            "quarantined": result.quarantined,
            "errors": len(result.errors),
            "gaps_remaining": len(ctx.manifest.gaps("eod")),
        },
    )
    log.info(
        "eod: published=%d quarantined=%d errors=%d outcome=%s",
        result.published,
        result.quarantined,
        len(result.errors),
        result.outcome,
    )
    return result


def _collect_eod_day(ctx: Context, day: date, *, run_id: str) -> checks.PublishResult:
    """Fetch, parse, check and publish one EOD trading day."""
    response = ctx.client.grouped_daily(day, include_otc=False)
    ingested_at = manifest_module.utcnow()
    parsed = parse.eod(response.results, day=day, ingested_at=ingested_at)

    batch_id = f"eod-{day.isoformat()}-r{ctx.manifest.next_revision('eod', day)}"
    split_tickers, split_ratios = splits_for_day(ctx, day)
    context = CheckContext(
        day=day,
        calendar=ctx.calendar,
        trailing_ticker_median=_trailing_ticker_median(ctx, day),
        eod_count_soft_tolerance=ctx.config.checks.eod_count_soft_tolerance,
        eod_count_hard_tolerance=ctx.config.checks.eod_count_hard_tolerance,
        previous_closes=_previous_closes(ctx, day),
        split_tickers=split_tickers,
        split_ratios=split_ratios,
        rounded_by_column=parsed.rounded,
    )
    return checks.validate_and_publish(
        parsed.table,
        dataset="eod",
        batch_id=batch_id,
        paths=ctx.paths,
        manifest=ctx.manifest,
        destination=ctx.paths.eod_path(day),
        context=context,
        source="massive:grouped_daily",
        params={
            "date": day.isoformat(),
            "adjusted": False,
            "include_otc": False,
            "request_id": response.request_id,
        },
        filename=f"eod_{day.isoformat()}.parquet",
        compression_level=ctx.compression_level,
        row_group_bytes=ctx.row_group_bytes,
        run_id=run_id,
        merge=True,  # EOD is partitioned monthly
    )


def _trailing_ticker_median(ctx: Context, day: date) -> int | None:
    """Median ticker count over the trailing published days.

    Reads ``source_rows`` -- the count the provider returned for that one day --
    not ``rows``, which for a monthly EOD file is the month to date. Comparing a
    day against month-to-date totals is what made a live run reject a good day
    for being "49.9% off" its own trailing median.

    Read from the manifest rather than by opening Parquet files: the counts are
    already recorded there, and a median over 20 days should not cost 20 reads.
    """
    counts = [
        row["source_rows"]
        for row in ctx.manifest.conn.execute(
            """
            SELECT source_rows FROM batches
             WHERE dataset = 'eod' AND status = 'published' AND partition_day < ?
               AND source_rows IS NOT NULL
             ORDER BY partition_day DESC LIMIT ?
            """,
            (day.isoformat(), TICKER_COUNT_WINDOW),
        )
    ]
    # With fewer than three days there is no meaningful median to compare to, and
    # inventing one would reject the first days of a fresh backfill.
    if len(counts) < 3:
        return None
    return int(statistics.median(counts))


def _previous_closes(ctx: Context, day: date) -> dict[str, int]:
    """Closes from the previous published trading day, for the large-move check."""
    previous = ctx.calendar.previous_trading_day(day)
    if previous is None:
        return {}
    path = ctx.paths.eod_path(previous)
    if not path.exists():
        return {}
    try:
        table = storage.read_parquet(path)
    except OSError:
        return {}
    import pyarrow.compute as pc

    same_day = table.filter(pc.equal(table.column("date"), previous))
    return dict(
        zip(
            same_day.column("ticker").to_pylist(),
            same_day.column("close").to_pylist(),
            strict=True,
        )
    )


# --------------------------------------------------------------------------- #
# Reference data
# --------------------------------------------------------------------------- #


def run_reference(ctx: Context, *, today: date | None = None) -> JobResult:
    """Snapshot every ticker, active and inactive, for one day.

    Snapshots are taken per trading day and never overwritten, so the series of
    files *is* the point-in-time record: a later reader can reconstruct the
    investable universe as it stood on any past day, including names that have
    since delisted.

    Only the current snapshot can be taken -- the provider serves present-day
    reference data, not historical -- so a day missed at the time is a permanent
    hole. Past days are therefore retired as ``skipped`` with a reason rather than
    left as gaps the job would retry forever.
    """
    today = today or date.today()
    run_id = f"reference-{uuid.uuid4().hex[:12]}"
    result = JobResult(job="reference", run_id=run_id)
    ctx.manifest.start_run(run_id, "reference")

    if not ctx.calendar.is_trading_day(today):
        log.info("reference: %s is not a trading day, nothing to snapshot", today)
        ctx.manifest.finish_run(run_id, "ok", {"reason": "not a trading day"})
        return result

    ctx.manifest.expect("tickers", [today])
    _retire_unreachable_snapshots(ctx, today)

    if not [g for g in ctx.manifest.gaps("tickers") if g.day == today]:
        log.info("reference: %s already snapshotted", today)
        ctx.manifest.finish_run(run_id, "ok", {"reason": "already present"})
        return result

    try:
        rows = ctx.client.tickers()  # active and inactive
    except (SourceError, PartialResponse) as exc:
        log.warning("reference: %s", exc)
        result.errors.append(str(exc))
        ctx.manifest.mark_coverage("tickers", today, manifest_module.FAILED, error=str(exc)[:500])
    else:
        parsed = parse.tickers(rows, snapshot=today, ingested_at=manifest_module.utcnow())
        revision = ctx.manifest.next_revision("tickers", today)
        outcome = checks.validate_and_publish(
            parsed.table,
            dataset="tickers",
            batch_id=f"tickers-{today.isoformat()}-r{revision}",
            paths=ctx.paths,
            manifest=ctx.manifest,
            destination=ctx.paths.tickers_path(today),
            context=CheckContext(day=today, calendar=ctx.calendar),
            source="massive:reference_tickers",
            params={"market": "stocks", "active": None, "snapshot_date": today.isoformat()},
            filename=f"tickers_{today.isoformat()}.parquet",
            compression_level=ctx.compression_level,
            row_group_bytes=ctx.row_group_bytes,
            run_id=run_id,
        )
        if outcome.published:
            result.published += 1
            result.rows += outcome.rows
            result.soft_flags.extend(f.message for f in outcome.report.soft_flags)
        else:
            result.quarantined += 1
            result.quarantine_reasons.append(outcome.report.summary())

    ctx.manifest.finish_run(
        run_id, result.outcome, {"published": result.published, "rows": result.rows}
    )
    log.info("reference: %s rows=%d outcome=%s", today, result.rows, result.outcome)
    return result


def _retire_superseded_actions(ctx: Context, dataset: str, today: date) -> int:
    """Stop reporting past corporate-action failures as owed work.

    Unlike the bar datasets these are not fetched per day: every run asks for one
    lookback window ending today. So nothing will ever retry the day a fetch
    failed on -- it is either re-covered by this run (the normal case, because the
    window overlaps) or permanently missed. Leaving the row pending would report a
    gap on every run forever, which is what made `1 still owed` outlive the
    failure that caused it.

    Called only after a fetch has actually succeeded, so a run that fails too
    leaves yesterday's failure on the books.
    """
    cutoff = today - timedelta(days=CORPORATE_ACTION_LOOKBACK_DAYS)
    retired = 0
    for gap in [g for g in ctx.manifest.gaps(dataset) if g.day < today]:
        covered = gap.day >= cutoff
        ctx.manifest.mark_coverage(
            dataset,
            gap.day,
            manifest_module.SKIPPED,
            error=ACTIONS_SUPERSEDED if covered else ACTIONS_MISSED,
            increment_attempt=False,
        )
        retired += 1
        if not covered:
            log.warning(
                "corporate_actions: %s %s was never collected and is now outside the "
                "%d-day lookback; revisions for that date are missing",
                dataset,
                gap.day,
                CORPORATE_ACTION_LOOKBACK_DAYS,
            )
    if retired:
        log.info("corporate_actions: %s: retired %d past day(s) no longer owed", dataset, retired)
    return retired


def _retire_unreachable_snapshots(ctx: Context, today: date) -> None:
    """Mark past reference days as permanently skipped rather than owed.

    There is no historical reference endpoint, so a day not snapshotted at the
    time can never be filled. Leaving it ``pending`` would report a gap on every
    run forever; ``skipped`` with the reason keeps both the record honest and the
    gap list meaningful.
    """
    stale = [g for g in ctx.manifest.gaps("tickers") if g.day < today]
    for gap in stale:
        ctx.manifest.mark_coverage(
            "tickers",
            gap.day,
            manifest_module.SKIPPED,
            error="reference data is only served as of today; this day cannot be backfilled",
            increment_attempt=False,
        )
    if stale:
        log.info("reference: retired %d unreachable past snapshot day(s)", len(stale))


# --------------------------------------------------------------------------- #
# Corporate actions
# --------------------------------------------------------------------------- #

#: Re-fetch this far back on every run. Splits and dividends are announced ahead
#: of time and later revised, so a window starting at the newest row we hold
#: would never see a correction to one already recorded.
CORPORATE_ACTION_LOOKBACK_DAYS = 90

#: Reasons recorded against corporate-action days that are no longer owed.
ACTIONS_SUPERSEDED = "superseded by a later fetch whose lookback covered this date"
ACTIONS_MISSED = (
    "older than the corporate-action lookback when the next fetch succeeded; "
    "revisions published for this date were never collected"
)


def run_corporate_actions(ctx: Context, *, today: date | None = None) -> JobResult:
    """Fetch new and revised splits and dividends into cumulative tables.

    Unlike the bar datasets these are not partitioned by day: each is one table
    keyed by (ticker, date), re-fetched incrementally and merged, so a revision
    replaces the row it corrects instead of duplicating it.
    """
    today = today or date.today()
    run_id = f"corpactions-{uuid.uuid4().hex[:12]}"
    result = JobResult(job="corporate_actions", run_id=run_id)
    ctx.manifest.start_run(run_id, "corporate_actions")

    # Consistent with the other two reference jobs, which skip non-trading days
    # rather than record one. The lookback window is 90 days, so anything
    # announced or revised over a weekend arrives with Monday's run.
    if not ctx.calendar.is_trading_day(today):
        log.info("corporate_actions: %s is not a trading day, nothing to fetch", today)
        ctx.manifest.finish_run(run_id, "ok", {"reason": "not a trading day"})
        return result

    since = today - timedelta(days=CORPORATE_ACTION_LOOKBACK_DAYS)
    plan = (
        ("splits", ctx.paths.splits_path()),
        ("dividends", ctx.paths.dividends_path()),
    )

    for dataset, destination in plan:
        try:
            if dataset == "splits":
                rows = ctx.client.splits(executed_after=since)
                parsed = parse.splits(rows, ingested_at=manifest_module.utcnow())
            else:
                rows = ctx.client.dividends(ex_after=since)
                parsed = parse.dividends(rows, ingested_at=manifest_module.utcnow())
        except (SourceError, PartialResponse) as exc:
            log.warning("corporate_actions: %s: %s", dataset, exc)
            result.errors.append(f"{dataset}: {exc}")
            ctx.manifest.expect(dataset, [today])
            ctx.manifest.mark_coverage(dataset, today, manifest_module.FAILED, error=str(exc)[:500])
            continue

        ctx.manifest.expect(dataset, [today])
        revision = ctx.manifest.next_revision(dataset, today)
        outcome = checks.validate_and_publish(
            parsed.table,
            dataset=dataset,
            batch_id=f"{dataset}-{today.isoformat()}-r{revision}",
            paths=ctx.paths,
            manifest=ctx.manifest,
            destination=destination,
            context=CheckContext(
                day=today, calendar=ctx.calendar, rounded_by_column=parsed.rounded
            ),
            source=f"massive:reference_{dataset}",
            params={"since": since.isoformat(), "fetched_for": today.isoformat()},
            filename=f"{dataset}.parquet",
            compression_level=ctx.compression_level,
            row_group_bytes=ctx.row_group_bytes,
            run_id=run_id,
            merge=True,  # cumulative table, not a daily partition
        )
        if outcome.published:
            result.published += 1
            result.rows += outcome.rows
            result.soft_flags.extend(f"{dataset}: {f.message}" for f in outcome.report.soft_flags)
            log.info("corporate_actions: %s %d row(s) in batch", dataset, outcome.rows)
            # This fetch covered the lookback window, so an earlier day left
            # pending or failed is not work anyone will do again.
            result.skipped += _retire_superseded_actions(ctx, dataset, today)
        else:
            result.quarantined += 1
            result.quarantine_reasons.append(f"{dataset}: {outcome.report.summary()}")

    ctx.manifest.finish_run(
        run_id, result.outcome, {"published": result.published, "rows": result.rows}
    )
    log.info("corporate_actions: outcome=%s rows=%d", result.outcome, result.rows)
    return result


def splits_for_day(ctx: Context, day: date) -> tuple[set[str], dict[str, float]]:
    """Tickers with a split executing on ``day``, and their ratios.

    Feeds the EOD large-move check: a 50%+ close move with a matching split is
    expected and goes unreported, while one without is flagged for review.
    Without this the check flags 14-18 tickers a day that are simply splits.
    """
    path = ctx.paths.splits_path()
    if not path.exists():
        return set(), {}
    try:
        table = storage.read_parquet(path)
    except OSError:
        return set(), {}

    import pyarrow.compute as pc

    same_day = table.filter(pc.equal(table.column("execution_date"), day))
    tickers = same_day.column("ticker").to_pylist()
    # split_from/split_to are floats as the provider sends them; the ratio is how
    # much the share count multiplied, so a 1:10 split gives 10.0.
    ratios = {
        ticker: (to_ / from_ if from_ else 0.0)
        for ticker, from_, to_ in zip(
            tickers,
            same_day.column("split_from").to_pylist(),
            same_day.column("split_to").to_pylist(),
            strict=True,
        )
    }
    return set(tickers), ratios


# --------------------------------------------------------------------------- #
# Minute bars
# --------------------------------------------------------------------------- #

#: Trading days to request per aggregate call during a backfill. One call returns
#: up to 50,000 bars; a liquid ticker with extended hours produces at most 960
#: bars a day (04:00-20:00), so 40 days is ~38,400 -- comfortably under the cap
#: with room for the densest names. Measured: AAPL on 2026-09-29 had 871 bars.
BACKFILL_CHUNK_DAYS = 40

#: Minimum chunk when narrowing after hitting the bar cap.
MIN_CHUNK_DAYS = 1


def run_minute(
    ctx: Context,
    *,
    today: date | None = None,
    backfill_days: int | None = None,
    max_calls: int | None = None,
    chunk_days: int = 1,
    tickers: Sequence[str] | None = None,
) -> JobResult:
    """Fill missing minute ticker-days for the universe.

    A published minute file covers one whole day for every ticker, but it is
    assembled from ~1,000 separate API calls. So each ticker's bars are written to
    ``work/minute/<date>/<ticker>.parquet`` as they arrive and the ticker-day is
    marked ``ok`` in coverage; the day is merged, validated and published only
    once nothing is outstanding for it. That is what makes a backfill resumable:
    a container killed half way through a day restarts with the fetched parts
    still on disk and the manifest knowing exactly which tickers remain.

    ``chunk_days`` > 1 switches to the backfill strategy: one call covers a range
    of days for a ticker and the response is split across the daily partitions.
    That is roughly a tenth of the calls, which is the difference between a
    two-year backfill taking days and taking weeks.

    ``max_calls`` bounds one invocation so the daily job cannot run indefinitely;
    whatever is left is picked up by the next run.
    """
    today = today or date.today()
    run_id = f"minute-{uuid.uuid4().hex[:12]}"
    result = JobResult(job="minute", run_id=run_id)
    ctx.manifest.start_run(run_id, "minute")

    result.skipped += retire_aged_out(ctx, "minute", today=today)
    days = expected_minute_days(ctx, today=today, backfill_days=backfill_days)
    if not days:
        log.info("minute: no expected trading days in the window")
        ctx.manifest.finish_run(run_id, "ok", {"reason": "no expected days"})
        return result

    universe = effective_universe(ctx, today)
    if not universe:
        message = (
            "no universe snapshot to collect against; run the universe job first "
            "(it derives from published EOD data)"
        )
        log.warning("minute: %s", message)
        result.errors.append(message)
        ctx.manifest.finish_run(run_id, result.outcome, {"reason": message})
        return result

    # The day owes the whole universe regardless of what this run will fetch, so
    # `tickers` narrows the work rather than redefining completeness. Otherwise a
    # targeted re-fetch would publish a day with universe members missing.
    ctx.manifest.expect("minute", days, tickers=universe)
    working = [t for t in universe if t in set(tickers)] if tickers else list(universe)
    log.info(
        "minute: %d day(s) x %d ticker(s) expected, %d worked, chunk=%dd, budget=%s call(s)",
        len(days),
        len(universe),
        len(working),
        chunk_days,
        max_calls if max_calls is not None else "unbounded",
    )

    if chunk_days > 1:
        calls = _backfill_minute(ctx, result, days, working, chunk_days, max_calls, run_id, today)
    else:
        calls = _collect_minute_daily(ctx, result, days, max_calls, run_id, set(working), today)

    # Publish any day that is now complete, including ones finished by an earlier
    # interrupted run. One grouped count for the lot: asking per day is quadratic
    # in the number of outstanding ticker-days, which during a backfill is most
    # of half a million.
    outstanding = ctx.manifest.outstanding_by_day("minute")
    for day in days:
        if outstanding.get(day):
            continue
        if ctx.manifest.latest_published("minute", day) is not None:
            continue
        _publish_minute_day(ctx, result, day, run_id=run_id)

    remaining = sum(ctx.manifest.outstanding_by_day("minute").values())
    ctx.manifest.finish_run(
        run_id,
        result.outcome,
        {
            "calls": calls,
            "published_days": result.published,
            "quarantined": result.quarantined,
            "ticker_days_remaining": remaining,
        },
    )
    log.info(
        "minute: %d call(s), %d day(s) published, %d ticker-day(s) still owed",
        calls,
        result.published,
        remaining,
    )
    return result


def expected_minute_days(
    ctx: Context, *, today: date, backfill_days: int | None = None
) -> list[date]:
    """Trading days minute data is expected for, ending yesterday."""
    window = backfill_days if backfill_days is not None else ctx.config.collection.backfill_days
    end = today - timedelta(days=1)
    start = max(end - timedelta(days=window), history_floor(ctx, "minute", today=today))
    return ctx.calendar.trading_days(start, end)


def _collect_minute_daily(
    ctx: Context,
    result: JobResult,
    days: Sequence[date],
    max_calls: int | None,
    run_id: str,
    wanted_tickers: set[str],
    today: date,
) -> int:
    """One call per ticker-day. The daily path, and the simple one."""
    calls = 0
    wanted_days = set(days)
    floor = ctx.manifest.history_floor("minute")
    if floor is not None:
        wanted_days = {d for d in wanted_days if d >= floor}
    for gap in ctx.manifest.gaps("minute"):
        if gap.day not in wanted_days or gap.ticker not in wanted_tickers:
            continue
        if max_calls is not None and calls >= max_calls:
            log.info("minute: call budget reached; the rest resumes on the next run")
            break
        calls += 1
        try:
            _fetch_minute_range(ctx, gap.ticker, gap.day, gap.day, run_id=run_id)
        except NotAuthorizedError:
            # One refusal, one probe, then every dead date retired at once --
            # otherwise the whole budget goes on calls that cannot succeed.
            retired = note_unauthorized(ctx, "minute", gap.day, today=today)
            result.skipped += retired
            floor = ctx.manifest.history_floor("minute") or gap.day
            wanted_days = {d for d in wanted_days if d >= floor}
            continue
        except (SourceError, PartialResponse) as exc:
            message = f"{gap.day.isoformat()} {gap.ticker}: {exc}"
            log.warning("minute: %s", message)
            result.errors.append(message)
            ctx.manifest.mark_coverage(
                "minute", gap.day, manifest_module.FAILED, ticker=gap.ticker, error=str(exc)[:500]
            )
    return calls


def _backfill_minute(
    ctx: Context,
    result: JobResult,
    days: Sequence[date],
    universe: Sequence[str],
    chunk_days: int,
    max_calls: int | None,
    run_id: str,
    today: date,
) -> int:
    """One call per ticker per date range, split across daily partitions.

    Groups the outstanding work by ticker so a wide range can be requested once.
    A range that comes back at the provider's 50,000-bar cap is indistinguishable
    from a truncated one, so the chunk is halved and retried rather than trusted.
    """
    outstanding: dict[str, list[date]] = {}
    wanted = set(days)
    for gap in ctx.manifest.gaps("minute"):
        if gap.day in wanted and gap.ticker:
            outstanding.setdefault(gap.ticker, []).append(gap.day)

    calls = 0
    for ticker in universe:
        pending = sorted(outstanding.get(ticker, ()))
        while pending:
            if max_calls is not None and calls >= max_calls:
                log.info("minute: call budget reached mid-backfill")
                return calls
            chunk = pending[:chunk_days]
            start, end = chunk[0], chunk[-1]
            width = chunk_days
            while True:
                calls += 1
                try:
                    _fetch_minute_range(ctx, ticker, start, end, days=chunk, run_id=run_id)
                except PartialResponse as exc:
                    if width <= MIN_CHUNK_DAYS:
                        result.errors.append(f"{ticker} {start}..{end}: {exc}")
                        for day in chunk:
                            ctx.manifest.mark_coverage(
                                "minute",
                                day,
                                manifest_module.FAILED,
                                ticker=ticker,
                                error=str(exc)[:500],
                            )
                        break
                    # At the bar cap: ask for less rather than store a truncation.
                    width = max(width // 2, MIN_CHUNK_DAYS)
                    chunk = pending[:width]
                    start, end = chunk[0], chunk[-1]
                    log.info("minute: %s hit the bar cap, narrowing to %dd", ticker, width)
                    continue
                except NotAuthorizedError:
                    retired = note_unauthorized(ctx, "minute", end, today=today)
                    result.skipped += retired
                    floor = ctx.manifest.history_floor("minute") or end
                    pending = [d for d in pending if d >= floor]
                    chunk = []
                    break
                except SourceError as exc:
                    result.errors.append(f"{ticker} {start}..{end}: {exc}")
                    for day in chunk:
                        ctx.manifest.mark_coverage(
                            "minute",
                            day,
                            manifest_module.FAILED,
                            ticker=ticker,
                            error=str(exc)[:500],
                        )
                    break
                break
            if not chunk:
                # The chunk was retired wholesale by the history floor; `pending`
                # has already been narrowed, so do not also slice it here.
                continue
            pending = pending[len(chunk) :]
    return calls


def _fetch_minute_range(
    ctx: Context,
    ticker: str,
    start: date,
    end: date,
    *,
    days: Sequence[date] | None = None,
    run_id: str,
) -> None:
    """Fetch one ticker over a date range and write a part per covered day.

    A day with no bars is marked ``skipped`` with a reason rather than left owed:
    an illiquid name genuinely does not trade some days, and retrying it forever
    would keep the day from ever being publishable.
    """
    response = ctx.client.aggregates(
        ticker,
        start,
        end,
        timespan="minute",
        multiplier=1,
    )
    ingested_at = manifest_module.utcnow()
    covered = list(days) if days is not None else [start]

    by_day: dict[date, list[dict]] = {day: [] for day in covered}
    for row in response.results:
        if "t" not in row:
            continue
        ts = datetime.fromtimestamp(row["t"] / 1000, tz=UTC)
        # A bar belongs to the exchange day its timestamp falls in, which is not
        # the UTC date: 00:30 UTC is the previous evening in New York.
        local_day = ts.astimezone(EXCHANGE_TZ).date()
        if local_day in by_day:
            by_day[local_day].append(row)

    floor = ctx.manifest.history_floor("minute")
    # One transaction for the whole chunk. `synchronous=FULL` fsyncs on every
    # commit, so marking 40 ticker-days individually meant 40 fsyncs per API call
    # -- tens of thousands per run, on an Unraid share where that is not free.
    marks: list[manifest_module.CoverageMark] = []
    for day, rows in by_day.items():
        if not rows:
            # A range straddling the window edge returns OK and silently clips, so
            # an empty day below the floor is aged out rather than simply quiet.
            aged_out = floor is not None and day < floor
            marks.append(
                manifest_module.CoverageMark(
                    day=day,
                    state=manifest_module.SKIPPED,
                    ticker=ticker,
                    error=AGED_OUT if aged_out else "no bars returned for this ticker-day",
                )
            )
            continue
        parsed = parse.minute(
            rows,
            ticker=ticker,
            day=day,
            calendar=ctx.calendar,
            ingested_at=ingested_at,
            include_extended_hours=ctx.config.collection.extended_hours,
        )
        if parsed.table.num_rows == 0:
            marks.append(
                manifest_module.CoverageMark(
                    day=day,
                    state=manifest_module.SKIPPED,
                    ticker=ticker,
                    error="all bars fell outside the session window",
                )
            )
            continue
        part = ctx.paths.minute_part(day, ticker)
        storage.write_parquet(
            storage.normalise(parsed.table, "minute"),
            part,
            dataset="minute",
            compression_level=ctx.compression_level,
            row_group_bytes=ctx.row_group_bytes,
        )
        marks.append(
            manifest_module.CoverageMark(
                day=day, state=manifest_module.OK, ticker=ticker, batch_id=run_id
            )
        )

    # After the parts are on disk: a crash between the two re-fetches the chunk,
    # which is one call, where the reverse would record data that is not there.
    ctx.manifest.mark_coverage_many("minute", marks)


def _publish_minute_day(ctx: Context, result: JobResult, day: date, *, run_id: str) -> None:
    """Merge a day's finished parts into one file, validate it, and publish."""
    parts = sorted(ctx.paths.minute_parts_dir(day).glob("*.parquet"))
    if not parts:
        return

    tables = [storage.read_parquet(part) for part in parts]
    table = storage.normalise(pa.concat_tables(tables), "minute")

    eod_table = None
    eod_path = ctx.paths.eod_path(day)
    if eod_path.exists():
        import pyarrow.compute as pc

        published_eod = storage.read_parquet(eod_path)
        eod_table = published_eod.filter(pc.equal(published_eod.column("date"), day))

    context = CheckContext(
        day=day,
        calendar=ctx.calendar,
        universe=effective_universe(ctx, day),
        explained_absences=_minute_absences(ctx, day),
        eod_table=eod_table,
    )
    revision = ctx.manifest.next_revision("minute", day)
    outcome = checks.validate_and_publish(
        table,
        dataset="minute",
        batch_id=f"minute-{day.isoformat()}-r{revision}",
        paths=ctx.paths,
        manifest=ctx.manifest,
        destination=ctx.paths.minute_path(day),
        context=context,
        source="massive:aggregates_minute",
        params={
            "date": day.isoformat(),
            "adjusted": False,
            "tickers": len(parts),
            "extended_hours": ctx.config.collection.extended_hours,
        },
        filename=f"minute_{day.isoformat()}.parquet",
        compression_level=ctx.compression_level,
        row_group_bytes=ctx.row_group_bytes,
        run_id=run_id,
        track_coverage=False,  # per-ticker; see validate_and_publish
    )
    if outcome.published:
        result.published += 1
        result.rows += outcome.rows
        result.soft_flags.extend(
            f"{day.isoformat()}: {f.message}" for f in outcome.report.soft_flags
        )
        # Parts have served their purpose; the published file is authoritative.
        shutil.rmtree(ctx.paths.minute_parts_dir(day), ignore_errors=True)
        log.info(
            "minute: published %s (%d ticker(s), %d bars, %.1f MB)",
            day,
            len(parts),
            outcome.rows,
            (ctx.paths.minute_path(day).stat().st_size / 1e6),
        )
    else:
        result.quarantined += 1
        result.quarantine_reasons.append(f"{day.isoformat()}: {outcome.report.summary()}")
        log.error("minute: %s quarantined: %s", day, outcome.report.summary())


def _minute_absences(ctx: Context, day: date) -> dict[str, str]:
    """Tickers legitimately absent for a day, with the recorded reason."""
    return {
        gap.ticker: gap.last_error or "skipped"
        for gap in ctx.manifest.gaps("minute", states=(manifest_module.SKIPPED,))
        if gap.day == day and gap.ticker
    }


# --------------------------------------------------------------------------- #
# Universe
# --------------------------------------------------------------------------- #


def dollar_volume_by_ticker(ctx: Context, days: Sequence[date]) -> dict[str, int]:
    """Mean daily dollar volume per ticker over ``days``, in scaled units.

    Computed in Python ints rather than with Arrow arithmetic because the
    intermediate overflows: ``close`` and ``volume`` are each scaled by 1e6, so
    their product carries 1e12 and a liquid megacap reaches ~1.3e22 -- three
    orders of magnitude past int64's 9.2e18. Dividing the scale back out as we go
    keeps every value in range and exact. 13,000 tickers over 20 days is ~260,000
    rows, which takes milliseconds.
    """
    scale = money.SCALE
    totals: dict[str, int] = {}
    counts: dict[str, int] = {}

    for path in {ctx.paths.eod_path(day) for day in days}:
        if not path.exists():
            continue
        table = storage.read_parquet(path)
        wanted = set(days)
        tickers = table.column("ticker").to_pylist()
        dates = table.column("date").to_pylist()
        closes = table.column("close").to_pylist()
        volumes = table.column("volume").to_pylist()
        for ticker, day, close, volume in zip(tickers, dates, closes, volumes, strict=True):
            if day not in wanted or not close or not volume:
                continue
            # (close/scale) * (volume/scale) * scale, exactly, without overflow.
            totals[ticker] = totals.get(ticker, 0) + (close * volume) // scale
            counts[ticker] = counts.get(ticker, 0) + 1

    return {ticker: total // counts[ticker] for ticker, total in totals.items()}


def run_universe(ctx: Context, *, today: date | None = None, size: int | None = None) -> JobResult:
    """Rebuild the minute universe: the top N tickers by trailing dollar volume.

    Built from published EOD data rather than a separate API call, so it costs no
    rate-limit budget and is reproducible from what is already on disk. Stored as
    a dated snapshot and never overwritten, so the universe as it stood on any
    past date can be recovered -- which is what keeps a later backtest honest
    about what it could have known.
    """
    today = today or date.today()
    size = size or ctx.config.universe.size
    run_id = f"universe-{uuid.uuid4().hex[:12]}"
    result = JobResult(job="universe", run_id=run_id)
    ctx.manifest.start_run(run_id, "universe")

    if not ctx.calendar.is_trading_day(today):
        log.info("universe: %s is not a trading day", today)
        ctx.manifest.finish_run(run_id, "ok", {"reason": "not a trading day"})
        return result

    ctx.manifest.expect("universe", [today])
    if not [g for g in ctx.manifest.gaps("universe") if g.day == today]:
        log.info("universe: %s already built", today)
        ctx.manifest.finish_run(run_id, "ok", {"reason": "already present"})
        return result

    window = ctx.calendar.recent_trading_days(
        ctx.calendar.previous_trading_day(today) or today, TICKER_COUNT_WINDOW
    )
    ranked = dollar_volume_by_ticker(ctx, window)
    if not ranked:
        message = (
            f"no published EOD data for {window[0]}..{window[-1]}; "
            "the universe is derived from EOD, so collect that first"
        )
        log.warning("universe: %s", message)
        result.errors.append(message)
        ctx.manifest.mark_coverage("universe", today, manifest_module.FAILED, error=message)
        ctx.manifest.finish_run(run_id, result.outcome, {"reason": message})
        return result

    try:
        allowed = _allowed_types(ctx, today)
    except RuntimeError as exc:
        # A misconfigured filter is a job failure to be reported, not a traceback:
        # it has to reach status.json so the health check sees it.
        log.warning("universe: %s", exc)
        result.errors.append(str(exc))
        ctx.manifest.mark_coverage("universe", today, manifest_module.FAILED, error=str(exc)[:500])
        ctx.manifest.finish_run(run_id, result.outcome, {"reason": str(exc)})
        return result

    if allowed is not None:
        before = len(ranked)
        ranked = {t: v for t, v in ranked.items() if t in allowed}
        log.info(
            "universe: type filter %s kept %d of %d candidate(s)",
            list(ctx.config.universe.types),
            len(ranked),
            before,
        )
        if not ranked:
            message = (
                f"the type filter {list(ctx.config.universe.types)} matched no ticker in the "
                "latest reference snapshot; check the types against a published snapshot"
            )
            log.warning("universe: %s", message)
            result.errors.append(message)
            ctx.manifest.mark_coverage("universe", today, manifest_module.FAILED, error=message)
            ctx.manifest.finish_run(run_id, result.outcome, {"reason": message})
            return result

    top = sorted(ranked.items(), key=lambda kv: (-kv[1], kv[0]))[:size]
    ingested_at = manifest_module.utcnow()
    table = pa.table(
        {
            "ticker": [ticker for ticker, _ in top],
            "rank": list(range(1, len(top) + 1)),
            "dollar_volume": [value for _, value in top],
            "snapshot_date": [today] * len(top),
            "ingested_at": [ingested_at] * len(top),
        },
        schema=schemas.UNIVERSE_SCHEMA,
    )

    revision = ctx.manifest.next_revision("universe", today)
    outcome = checks.validate_and_publish(
        table,
        dataset="universe",
        batch_id=f"universe-{today.isoformat()}-r{revision}",
        paths=ctx.paths,
        manifest=ctx.manifest,
        destination=ctx.paths.universe_path(today),
        context=CheckContext(day=today, calendar=ctx.calendar),
        source="derived:eod_dollar_volume",
        params={
            "window_days": len(window),
            "window": f"{window[0].isoformat()}..{window[-1].isoformat()}",
            "size": size,
            "candidates": len(ranked),
            "types": list(ctx.config.universe.types) or None,
        },
        filename=f"universe_{today.isoformat()}.parquet",
        compression_level=ctx.compression_level,
        row_group_bytes=ctx.row_group_bytes,
        run_id=run_id,
    )
    if outcome.published:
        result.published += 1
        result.rows += outcome.rows
        result.soft_flags.extend(f.message for f in outcome.report.soft_flags)
        log.info(
            "universe: %d ticker(s) from %d candidate(s) over %d trading day(s)",
            outcome.rows,
            len(ranked),
            len(window),
        )
    else:
        result.quarantined += 1
        result.quarantine_reasons.append(outcome.report.summary())

    ctx.manifest.finish_run(run_id, result.outcome, {"rows": result.rows})
    return result


def _allowed_types(ctx: Context, day: date) -> set[str] | None:
    """Tickers whose instrument type is permitted, or ``None`` for no filter.

    Read from the most recent reference snapshot at or before ``day``, so the
    filter reflects what was known then rather than today. A configured filter
    with no snapshot to read is a hard failure rather than a silent pass-through:
    quietly ignoring it would admit the very instruments it was meant to exclude.
    """
    wanted = set(ctx.config.universe.types)
    if not wanted:
        return None

    best: tuple[date, Path] | None = None
    for path in sorted(ctx.paths.published.glob("reference/tickers_*.parquet")):
        try:
            when = date.fromisoformat(path.stem.removeprefix("tickers_"))
        except ValueError:
            continue
        if when <= day and (best is None or when > best[0]):
            best = (when, path)
    if best is None:
        raise RuntimeError(
            f"[universe] types={sorted(wanted)} needs a reference snapshot to filter on, "
            "and none is published at or before "
            f"{day.isoformat()}; run the reference job first"
        )

    table = storage.read_parquet(best[1])
    return {
        ticker
        for ticker, kind in zip(
            table.column("ticker").to_pylist(), table.column("type").to_pylist(), strict=True
        )
        if kind in wanted
    }


def latest_universe(ctx: Context, on_or_before: date) -> tuple[date, list[str]] | None:
    """The most recent universe snapshot at or before a date."""
    snapshots = sorted(ctx.paths.published.glob("reference/universe_*.parquet"))
    best: tuple[date, Path] | None = None
    for path in snapshots:
        try:
            when = date.fromisoformat(path.stem.removeprefix("universe_"))
        except ValueError:
            continue
        if when <= on_or_before and (best is None or when > best[0]):
            best = (when, path)
    if best is None:
        return None
    table = storage.read_parquet(best[1])
    return best[0], table.column("ticker").to_pylist()


def effective_universe(ctx: Context, day: date) -> list[str]:
    """Tickers to collect minute bars for on ``day``.

    The current universe plus anything that was in it within the last
    ``grace_days``. The plan's reason: a stock hovering at the cutoff would
    otherwise drop out and back in, leaving holes in its minute history that can
    never be refilled once the provider's 2-year window passes. Keeping recent
    leavers costs a few extra calls and avoids an unrepairable gap.
    """
    grace = ctx.config.universe.grace_days
    tickers: dict[str, None] = {}

    current = latest_universe(ctx, day)
    if current is not None:
        for ticker in current[1]:
            tickers[ticker] = None

    # Snapshots inside the grace window, so recent leavers stay covered.
    for path in sorted(ctx.paths.published.glob("reference/universe_*.parquet")):
        try:
            when = date.fromisoformat(path.stem.removeprefix("universe_"))
        except ValueError:
            continue
        if when > day or (day - when).days > grace:
            continue
        for ticker in storage.read_parquet(path).column("ticker").to_pylist():
            tickers.setdefault(ticker, None)

    return list(tickers)


# --------------------------------------------------------------------------- #
# Backups
# --------------------------------------------------------------------------- #


def run_backup(ctx: Context, *, today: date | None = None) -> JobResult:
    """Write a verified backup, then prune the oldest.

    Does nothing when no backup directory is configured, so the job can be
    registered unconditionally and enabled by configuration alone.
    """
    today = today or date.today()
    run_id = f"backup-{uuid.uuid4().hex[:12]}"
    result = JobResult(job="backup", run_id=run_id)
    ctx.manifest.start_run(run_id, "backup")

    directory = ctx.config.backup.directory
    if not directory:
        log.info("backup: no [backup] directory configured, skipping")
        ctx.manifest.finish_run(run_id, "ok", {"reason": "not configured"})
        return result

    target = Path(directory)
    try:
        info = backup_module.create(
            ctx.paths,
            target,
            include_data=ctx.config.backup.include_data,
        )
        problems = backup_module.verify(info.path)
        if problems:
            raise backup_module.BackupError("; ".join(problems[:5]))
        removed = backup_module.prune(target, ctx.config.backup.keep)
    except Exception as exc:
        # Deliberately broad: this job's contract is to report rather than crash,
        # and a backup destination can fail in many ways (permissions, a path that
        # is a file, a full disk, a bad path string). The failure reaches
        # status.json and the alerter either way.
        message = f"backup to {target} failed: {type(exc).__name__}: {exc}"
        log.error("backup: %s", message)
        result.errors.append(message)
    else:
        result.published += 1
        result.rows = info.files
        log.info(
            "backup: %s verified (%d file(s), %.1f MB), pruned %d",
            info.path.name,
            info.files,
            info.bytes / 1e6,
            len(removed),
        )

    ctx.manifest.finish_run(run_id, result.outcome, {"files": result.rows})
    return result


# --------------------------------------------------------------------------- #
# Daily summary
# --------------------------------------------------------------------------- #


def run_checks_report(ctx: Context, *, today: date | None = None) -> JobResult:
    """Write the daily summary into ``status.json``.

    Makes no API calls. Its job is to turn the manifest into the one file a
    dashboard or an operator reads: what each dataset holds, what it still owes,
    what was flagged, and how much disk is left.
    """
    today = today or date.today()
    run_id = f"report-{uuid.uuid4().hex[:12]}"
    result = JobResult(job="checks_report", run_id=run_id)
    ctx.manifest.start_run(run_id, "checks_report")

    current = status_module.read(ctx.paths.status_file) or status_module.Status()
    notes: list[str] = []
    for dataset in ("eod", "minute", "tickers", "splits", "dividends", "universe"):
        summary = ctx.manifest.coverage_summary(dataset)
        if not summary:
            continue
        owed = summary.get(manifest_module.PENDING, 0) + summary.get(manifest_module.FAILED, 0)
        notes.append(
            f"{dataset}: "
            + ", ".join(f"{k}={v}" for k, v in sorted(summary.items()))
            + (f"  ({owed} still owed)" if owed else "")
        )

    quarantined = ctx.manifest.conn.execute(
        "SELECT COUNT(*) AS n FROM batches WHERE status = ?", (manifest_module.QUARANTINED,)
    ).fetchone()["n"]
    if quarantined:
        notes.append(f"{quarantined} quarantined batch(es) awaiting review in quarantine/")

    flagged = ctx.manifest.conn.execute(
        """
        SELECT COUNT(*) AS n FROM batches
         WHERE status = ? AND reason IS NOT NULL
        """,
        (manifest_module.PUBLISHED,),
    ).fetchone()["n"]
    if flagged:
        notes.append(f"{flagged} published batch(es) carry soft flags")

    current.notes = notes
    current.disk = status_module.disk_usage(ctx.paths.root)
    status_module.write(current, ctx.paths.status_file)

    ctx.manifest.finish_run(run_id, "ok", {"notes": len(notes)})
    log.info("checks_report: %d note(s)", len(notes))
    for note in notes:
        log.info("  %s", note)
    return result


# --------------------------------------------------------------------------- #
# Status
# --------------------------------------------------------------------------- #


def write_status(ctx: Context, results: list[JobResult]) -> None:
    """Fold job results into ``status.json`` for the health check and dashboard."""
    current = status_module.read(ctx.paths.status_file) or status_module.Status()
    for result in results:
        job_status = result.to_status()
        coverage: dict[str, int] = {}
        for dataset in JOB_DATASETS.get(result.job, (result.job,)):
            for state, count in ctx.manifest.coverage_summary(dataset).items():
                coverage[state] = coverage.get(state, 0) + count
        job_status.coverage = coverage
        current.jobs[result.job] = job_status
    current.disk = status_module.disk_usage(ctx.paths.root)
    status_module.write(current, ctx.paths.status_file)
