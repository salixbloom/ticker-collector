"""M10 burn-in: has the collector run clean for N consecutive trading days?

M10's criterion is an observation over wall-clock time -- "10 consecutive trading
days with no hard failures, no unexplained gaps and no manual intervention" -- so
it cannot be satisfied by code. What code can do is define the criterion precisely
and measure it from the manifest, so the answer is a command rather than a
judgement call, and so a failure names the day and the reason.

A trading day counts as clean when, for every dataset the collector is expected to
have collected that day:

* nothing is still owed -- no ``pending`` or ``failed`` coverage;
* nothing was quarantined;
* the published batch verifies against its recorded checksums.

Soft flags do **not** disqualify a day. A real day flags ~8% of EOD tickers for
vwap and that is the provider's data, not a fault. The plan asks for "no
unexplained gaps", and an explained absence -- a ``skipped`` ticker-day with a
recorded reason -- is explained by construction.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from . import manifest as manifest_module
from .checks.trading_calendar import TradingCalendar
from .manifest import Manifest
from .storage import DataPaths

log = logging.getLogger(__name__)

#: Trading days the plan requires clean.
REQUIRED_CLEAN_DAYS = 10

#: Datasets judged per trading day. Reference and corporate actions are collected
#: "as of today" rather than for a past date, so they are reported separately
#: rather than counted against a historical day.
DAILY_DATASETS = ("eod", "minute")


@dataclass
class DayVerdict:
    """Whether one trading day is clean, and if not, why."""

    day: date
    clean: bool
    owed: dict[str, int] = field(default_factory=dict)
    quarantined: int = 0
    soft_flagged: int = 0
    #: Rows the provider returned for this day (`source_rows`), not the published
    #: artifact's count, which for a monthly EOD file is the month to date.
    rows: int = 0
    reasons: list[str] = field(default_factory=list)
    #: Datasets with no coverage rows at all for this day.
    not_expected: list[str] = field(default_factory=list)


@dataclass
class BurnInReport:
    """The burn-in verdict over a window of trading days."""

    required: int
    days: list[DayVerdict]
    checksum_problems: list[str] = field(default_factory=list)
    #: Bytes actually on disk, measured from the published files rather than summed
    #: from batch records: several EOD batches share one monthly file, so adding
    #: their recorded sizes would count the same file many times over.
    published_bytes: int = 0

    @property
    def consecutive_clean(self) -> int:
        """Clean days counting back from the most recent, stopping at the first fault.

        Counted backwards because that is what the criterion means: the run has to
        be unbroken *up to now*, not merely contain ten good days somewhere.
        """
        streak = 0
        for verdict in reversed(self.days):
            if not verdict.clean:
                break
            streak += 1
        return streak

    @property
    def passed(self) -> bool:
        return self.consecutive_clean >= self.required and not self.checksum_problems

    @property
    def total_rows(self) -> int:
        return sum(d.rows for d in self.days)

    @property
    def total_bytes(self) -> int:
        return self.published_bytes

    @property
    def soft_flag_rate(self) -> float:
        """Share of judged days carrying at least one soft flag."""
        if not self.days:
            return 0.0
        return sum(1 for d in self.days if d.soft_flagged) / len(self.days)

    def to_dict(self) -> dict[str, Any]:
        return {
            "required_clean_days": self.required,
            "consecutive_clean_days": self.consecutive_clean,
            "passed": self.passed,
            "window": (
                f"{self.days[0].day.isoformat()}..{self.days[-1].day.isoformat()}"
                if self.days
                else None
            ),
            "days_examined": len(self.days),
            "total_rows": self.total_rows,
            "total_bytes": self.total_bytes,
            "soft_flag_day_rate": round(self.soft_flag_rate, 4),
            "checksum_problems": self.checksum_problems,
            "faults": [
                {"day": d.day.isoformat(), "reasons": d.reasons} for d in self.days if not d.clean
            ],
        }


def evaluate(
    manifest: Manifest,
    paths: DataPaths,
    calendar: TradingCalendar,
    *,
    today: date | None = None,
    required: int = REQUIRED_CLEAN_DAYS,
    window: int | None = None,
    verify_checksums: bool = True,
) -> BurnInReport:
    """Judge the most recent trading days against the burn-in criterion."""
    today = today or date.today()
    span = window or required * 2
    # Ends yesterday: today's data does not exist until the session closes.
    candidates = calendar.recent_trading_days(calendar.previous_trading_day(today) or today, span)

    coverage = _coverage_by_day(manifest)
    batches = _batches_by_day(manifest)

    days: list[DayVerdict] = []
    for day in candidates:
        verdict = DayVerdict(day=day, clean=True)
        judged_any = False

        for dataset in DAILY_DATASETS:
            states = coverage.get((dataset, day))
            if not states:
                verdict.not_expected.append(dataset)
                continue
            judged_any = True
            owed = states.get(manifest_module.PENDING, 0) + states.get(manifest_module.FAILED, 0)
            if owed:
                verdict.owed[dataset] = owed
                verdict.clean = False
                verdict.reasons.append(f"{dataset}: {owed} unit(s) still owed")

        for batch in batches.get(day, ()):
            if batch["status"] == manifest_module.QUARANTINED:
                verdict.quarantined += 1
                verdict.clean = False
                verdict.reasons.append(
                    f"{batch['dataset']}: quarantined ({batch['reason'] or 'no reason recorded'})"
                )
            elif batch["status"] == manifest_module.PUBLISHED:
                # `source_rows` is what the provider returned for THIS day. `rows`
                # is the published artifact's count, which for a monthly EOD file
                # is the month to date -- summing that across days would count the
                # same rows over and over.
                verdict.rows += batch["source_rows"] or batch["rows"] or 0
                if batch["reason"]:
                    verdict.soft_flagged += 1

        if not judged_any:
            # A universe or ticker snapshot landing on this day does not make it a
            # collected trading day: the burn-in is about the daily bar datasets,
            # and `judged_any` is set only by those.
            verdict.clean = False
            verdict.reasons.append("no daily dataset collected for this day")

        days.append(verdict)

    problems: list[str] = []
    if verify_checksums:
        problems = [f"{batch_id}: {why}" for batch_id, why in manifest.verify_checksums(paths.root)]

    on_disk = sum(total for _count, total in _published_sizes(paths).values())
    return BurnInReport(
        required=required,
        days=days,
        checksum_problems=problems,
        published_bytes=on_disk,
    )


def _coverage_by_day(manifest: Manifest) -> dict[tuple[str, date], dict[str, int]]:
    out: dict[tuple[str, date], dict[str, int]] = {}
    for row in manifest.conn.execute(
        "SELECT dataset, day, state, COUNT(*) AS n FROM coverage GROUP BY dataset, day, state"
    ):
        try:
            day = date.fromisoformat(row["day"])
        except ValueError:
            continue
        out.setdefault((row["dataset"], day), {})[row["state"]] = row["n"]
    return out


def _batches_by_day(manifest: Manifest) -> dict[date, list[dict[str, Any]]]:
    """Batches per partition day, counting only the current revision of each.

    A day re-collected after a correction has several published revisions for the
    same (dataset, day); summing them would report the day's rows two or three
    times over. Quarantined batches are kept whatever their revision, because a
    quarantine is a fault that happened and should still be reported.
    """
    out: dict[date, list[dict[str, Any]]] = {}
    for row in manifest.conn.execute(
        """
        SELECT b.dataset, b.partition_day, b.status, b.rows, b.source_rows, b.bytes, b.reason
          FROM batches AS b
         WHERE b.partition_day IS NOT NULL
           AND (
                b.status != 'published'
                OR b.revision = (
                    SELECT MAX(revision) FROM batches
                     WHERE dataset = b.dataset
                       AND partition_day IS b.partition_day
                       AND status = 'published'
                )
           )
        """
    ):
        try:
            day = date.fromisoformat(row["partition_day"])
        except (ValueError, TypeError):
            continue
        out.setdefault(day, []).append(dict(row))
    return out


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #


def render(report: BurnInReport) -> str:
    """A human-readable burn-in status page."""
    lines: list[str] = []
    verdict = "PASS" if report.passed else "IN PROGRESS"
    lines.append(
        f"burn-in: {verdict} -- {report.consecutive_clean}/{report.required} "
        "consecutive clean trading days"
    )
    if report.days:
        lines.append(
            f"window: {report.days[0].day.isoformat()} .. {report.days[-1].day.isoformat()} "
            f"({len(report.days)} trading day(s) examined)"
        )
    lines.append("")
    lines.append(f"{'day':<12} {'clean':<6} {'rows':>10}  notes")
    lines.append("-" * 76)
    for day in report.days:
        note = (
            "; ".join(day.reasons)
            if day.reasons
            else (f"{day.soft_flagged} soft-flagged batch(es)" if day.soft_flagged else "")
        )
        if day.not_expected:
            note = (note + "; " if note else "") + f"not collected: {', '.join(day.not_expected)}"
        lines.append(
            f"{day.day.isoformat():<12} {'yes' if day.clean else 'NO':<6} {day.rows:>10,}  {note}"
        )

    lines.append("")
    lines.append(f"rows collected in window: {report.total_rows:,}")
    lines.append(f"published on disk:        {report.total_bytes / 1e6:,.1f} MB")
    lines.append(
        f"soft-flag rate:           {report.soft_flag_rate:.0%} of days carry at least one flag"
    )
    if report.checksum_problems:
        lines.append("")
        lines.append(f"CHECKSUM PROBLEMS ({len(report.checksum_problems)}):")
        for problem in report.checksum_problems[:10]:
            lines.append(f"  {problem}")
    if not report.passed:
        lines.append("")
        remaining = max(report.required - report.consecutive_clean, 0)
        lines.append(
            f"{remaining} more consecutive clean trading day(s) needed. "
            "Soft flags do not count against a day; owed units, quarantined batches "
            "and checksum problems do."
        )
    return "\n".join(lines)


def journal_entry(report: BurnInReport, paths: DataPaths) -> str:
    """The short write-up M10 asks for, as markdown."""
    from .storage import DataPaths as _DataPaths  # noqa: F401  (clarity of type)

    sizes = _published_sizes(paths)
    lines = [
        "# Burn-in write-up",
        "",
        f"- **Verdict:** {'PASS' if report.passed else 'IN PROGRESS'} "
        f"({report.consecutive_clean}/{report.required} consecutive clean trading days)",
    ]
    if report.days:
        lines.append(
            f"- **Window:** {report.days[0].day.isoformat()} .. "
            f"{report.days[-1].day.isoformat()} ({len(report.days)} trading days examined)"
        )
    lines += [
        "",
        "## Storage",
        "",
        "| Dataset | Files | Size |",
        "|---|---|---|",
    ]
    for dataset, (count, total) in sorted(sizes.items()):
        lines.append(f"| {dataset} | {count} | {total / 1e6:,.1f} MB |")
    grand = sum(total for _c, total in sizes.values())
    lines.append(
        f"| **total** | {sum(c for c, _t in sizes.values())} | **{grand / 1e6:,.1f} MB** |"
    )

    lines += [
        "",
        "## Check flags",
        "",
        f"- {report.soft_flag_rate:.0%} of judged days carry at least one soft flag.",
        "- Soft flags are expected, not faults: a real EOD day flags ~8% of tickers "
        "for vwap falling outside the day's high/low, which is the provider's data.",
        "",
        "## Issues found",
        "",
    ]
    faults = [d for d in report.days if not d.clean]
    if not faults:
        lines.append("None. No owed units, no quarantined batches, no checksum problems.")
    else:
        for day in faults:
            lines.append(f"- **{day.day.isoformat()}**: {'; '.join(day.reasons)}")
    if report.checksum_problems:
        lines.append("")
        lines.append("### Checksum problems")
        for problem in report.checksum_problems[:20]:
            lines.append(f"- {problem}")
    return "\n".join(lines) + "\n"


def _published_sizes(paths: DataPaths) -> dict[str, tuple[int, int]]:
    sizes: dict[str, tuple[int, int]] = {}
    if not paths.published.exists():
        return sizes
    for path in paths.published.rglob("*.parquet"):
        dataset = path.relative_to(paths.published).parts[0]
        count, total = sizes.get(dataset, (0, 0))
        sizes[dataset] = (count + 1, total + path.stat().st_size)
    return sizes


def to_json(report: BurnInReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)
