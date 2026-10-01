"""The individual checks.

Each takes a normalised table and returns findings. Everything is expressed with
``pyarrow.compute`` rather than Python loops: an EOD day is ~12,600 rows and a
minute day is ~400,000, and these run on every batch.

Severities follow the plan, with one deliberate change recorded in the decision
log: **vwap-within-[low, high] is soft, not hard.** Measured against a real
grouped-daily response (2026-09-29), 977 of 12,605 rows -- 7.8% -- have a vwap
outside the day's high/low range, and not marginally: VEEAW reported low 0.0202,
high 0.022, vwap 0.01723, 15% below the low. Off-exchange and odd-lot prints land
in the vwap but not the high/low. As a hard check this would quarantine 8% of
every day's tickers, so it flags instead.
"""

from __future__ import annotations

from datetime import date

import pyarrow as pa
import pyarrow.compute as pc

from .. import schemas
from ..money import format_scaled
from .base import Finding, hard, soft
from .trading_calendar import REGULAR_FULL_SESSION_BARS, TradingCalendar

#: Day-over-day close move beyond which a row is flagged for split review.
LARGE_MOVE_RATIO = 0.50

# Tolerances for reconciling minute bars against the EOD bar, each set from
# measurement rather than guessed. Four tickers of differing liquidity (AAPL,
# MSFT, F, SOFI) on 2026-09-29:
#
#   regular-session high/low vs EOD high/low   0.0000% on all four  -> tight
#   all-session transactions vs EOD n          -0.2% .. -0.7%       -> 3%
#   regular-session volume vs EOD v           -9.8% .. -28.6%       -> unusable
#   all-session volume vs EOD v               -4.6% .. -24.9%       -> unusable
#
#: High/low reconcile exactly, so this can be tight enough to catch real corruption.
PRICE_RECONCILE_TOLERANCE = 0.001
#: Transaction counts reconcile closely once pre- and after-hours are included.
TRANSACTION_RECONCILE_TOLERANCE = 0.03
#: Volume does not reconcile. The provider's daily bar carries share volume its
#: own minute bars do not account for -- consistently 5-29% more, for every
#: ticker tested. The plan specified a tolerance here; at any tight value this
#: would flag every ticker every day and become noise, so it is kept only as a
#: gross-breakage tripwire and the real volume check is the transaction count.
VOLUME_RECONCILE_TOLERANCE = 0.50


def _time_column(dataset: str) -> str:
    return "date" if dataset == "eod" else "ts"


def _samples(
    table: pa.Table,
    mask: pa.Array,
    columns: list[str],
    limit: int = 25,
    dataset: str = "eod",
) -> list[str]:
    """Render up to ``limit`` offending rows for the report.

    Scaled columns are rendered as decimals so a report reads ``close=337.804900``
    rather than ``close=337804900``. Which columns are scaled depends on the
    dataset -- splits' ratios are plain floats, not scaled integers.
    """
    scaled = set(schemas.SCALED_COLUMNS.get(dataset, ()))
    rows = table.filter(mask).select(columns).slice(0, limit).to_pylist()
    out = []
    for row in rows:
        parts = []
        for key, value in row.items():
            if key in scaled and isinstance(value, int):
                parts.append(f"{key}={format_scaled(value)}")
            else:
                parts.append(f"{key}={value}")
        out.append(" ".join(parts))
    return out


# --------------------------------------------------------------------------- #
# Structural (hard)
# --------------------------------------------------------------------------- #


def check_no_duplicate_keys(table: pa.Table, dataset: str) -> Finding | None:
    """No duplicate (ticker, time) rows.

    A duplicate means the same bar arrived twice -- a paging bug or a double
    append -- and would double-count volume for any reader.
    """
    ds = schemas.get(dataset)
    if table.num_rows == 0:
        return None
    keys = list(zip(*(table.column(c).to_pylist() for c in ds.key), strict=True))
    seen: set[tuple] = set()
    duplicates: list[tuple] = []
    for key in keys:
        if key in seen:
            duplicates.append(key)
        else:
            seen.add(key)
    if not duplicates:
        return None
    return hard(
        "no_duplicate_keys",
        f"{len(duplicates)} duplicate {ds.key} key(s)",
        samples=[str(k) for k in duplicates[:25]],
        count=len(duplicates),
    )


def check_timestamps_in_window(
    table: pa.Table, dataset: str, day: date, calendar: TradingCalendar
) -> Finding | None:
    """Every row's timestamp belongs to the requested day and its session window."""
    if table.num_rows == 0:
        return None
    column = _time_column(dataset)

    if dataset == "eod":
        wrong = pc.not_equal(table.column(column), pa.scalar(day, type=pa.date32()))
        bad = pc.sum(pc.cast(wrong, pa.int64())).as_py() or 0
        if bad:
            return hard(
                "timestamps_in_window",
                f"{bad} row(s) are not dated {day.isoformat()}",
                samples=_samples(table, wrong, ["ticker", column]),
                count=bad,
            )
        return None

    window = calendar.extended_window(day)
    if window is None:
        return hard("timestamps_in_window", f"{day.isoformat()} is not a trading day")
    start, end = window
    outside = pc.or_(
        pc.less(table.column(column), pa.scalar(start, type=table.schema.field(column).type)),
        pc.greater_equal(
            table.column(column), pa.scalar(end, type=table.schema.field(column).type)
        ),
    )
    bad = pc.sum(pc.cast(outside, pa.int64())).as_py() or 0
    if bad:
        return hard(
            "timestamps_in_window",
            f"{bad} bar(s) outside the {start.isoformat()}..{end.isoformat()} session window",
            samples=_samples(table, outside, ["ticker", column]),
            count=bad,
        )
    return None


def check_session_labels(table: pa.Table, day: date, calendar: TradingCalendar) -> Finding | None:
    """Minute bars' ``session`` column matches what the calendar says."""
    if table.num_rows == 0 or "session" not in table.column_names:
        return None
    timestamps = table.column("ts").to_pylist()
    labels = table.column("session").to_pylist()
    wrong = [
        f"ts={ts.isoformat()} labelled={got} expected={calendar.classify(ts, day)}"
        for ts, got in zip(timestamps, labels, strict=True)
        if (expected := calendar.classify(ts, day)) is not None and int(expected) != got
    ]
    if not wrong:
        return None
    return hard(
        "session_labels",
        f"{len(wrong)} bar(s) carry the wrong session label",
        samples=wrong[:25],
        count=len(wrong),
    )


# --------------------------------------------------------------------------- #
# Value sanity (hard, except vwap)
# --------------------------------------------------------------------------- #


def check_prices_positive(table: pa.Table, dataset: str) -> Finding | None:
    """All OHLC prices > 0. A zero or negative price is never real."""
    if table.num_rows == 0:
        return None
    mask = None
    for column in ("open", "high", "low", "close"):
        if column not in table.column_names:
            continue
        bad = pc.less_equal(table.column(column), 0)
        mask = bad if mask is None else pc.or_(mask, bad)
    if mask is None:
        return None
    count = pc.sum(pc.cast(mask, pa.int64())).as_py() or 0
    if not count:
        return None
    return hard(
        "prices_positive",
        f"{count} row(s) with a non-positive price",
        samples=_samples(
            table,
            mask,
            ["ticker", _time_column(dataset), "open", "high", "low", "close"],
            dataset=dataset,
        ),
        count=count,
    )


def check_volume_non_negative(table: pa.Table, dataset: str) -> Finding | None:
    """Volume >= 0. Zero is legitimate -- 3 of 12,605 EOD rows had it on a probed day."""
    if table.num_rows == 0 or "volume" not in table.column_names:
        return None
    mask = pc.less(table.column("volume"), 0)
    count = pc.sum(pc.cast(mask, pa.int64())).as_py() or 0
    if not count:
        return None
    return hard(
        "volume_non_negative",
        f"{count} row(s) with negative volume",
        samples=_samples(table, mask, ["ticker", _time_column(dataset), "volume"], dataset=dataset),
        count=count,
    )


def check_ohlc_ordering(table: pa.Table, dataset: str) -> Finding | None:
    """``low <= open <= high`` and ``low <= close <= high`` for every bar."""
    if table.num_rows == 0:
        return None
    required = {"open", "high", "low", "close"}
    if not required.issubset(table.column_names):
        return None
    low, high = table.column("low"), table.column("high")
    mask = pc.or_(
        pc.greater(low, high),
        pc.or_(
            pc.or_(pc.less(table.column("open"), low), pc.greater(table.column("open"), high)),
            pc.or_(pc.less(table.column("close"), low), pc.greater(table.column("close"), high)),
        ),
    )
    count = pc.sum(pc.cast(mask, pa.int64())).as_py() or 0
    if not count:
        return None
    return hard(
        "ohlc_ordering",
        f"{count} bar(s) violate low <= open/close <= high",
        samples=_samples(
            table,
            mask,
            ["ticker", _time_column(dataset), "open", "high", "low", "close"],
            dataset=dataset,
        ),
        count=count,
    )


def check_vwap_in_range(table: pa.Table, dataset: str) -> Finding | None:
    """vwap within [low, high] -- **soft**, see the module docstring."""
    if table.num_rows == 0 or "vwap" not in table.column_names:
        return None
    vwap = table.column("vwap")
    present = pc.is_valid(vwap)
    mask = pc.and_(
        present,
        pc.or_(pc.less(vwap, table.column("low")), pc.greater(vwap, table.column("high"))),
    )
    count = pc.sum(pc.cast(mask, pa.int64())).as_py() or 0
    if not count:
        return None
    share = count / table.num_rows
    return soft(
        "vwap_in_range",
        f"{count} row(s) ({share:.1%}) have vwap outside [low, high]; "
        "off-exchange and odd-lot prints are included in vwap but not in high/low",
        samples=_samples(table, mask, ["ticker", "low", "high", "vwap"], dataset=dataset),
        count=count,
    )


def check_precision_loss(rounded_by_column: dict[str, int]) -> Finding | None:
    """Values that needed more precision than the scale allows were rounded.

    Soft by decision: the data still publishes, but the loss is recorded rather
    than silent, so a provider widening its precision is visible in status.json.
    """
    affected = {name: n for name, n in rounded_by_column.items() if n}
    if not affected:
        return None
    total = sum(affected.values())
    return soft(
        "precision_loss",
        f"{total} value(s) rounded to fit the 1e6 scale",
        samples=[f"{name}: {n}" for name, n in sorted(affected.items())],
        count=total,
    )


def check_snapshot_date(table: pa.Table, day: date) -> Finding | None:
    """Every row of a snapshot carries the snapshot's own date.

    A snapshot whose rows claim a different date would silently corrupt the
    point-in-time record -- the thing that lets a later reader reconstruct the
    universe as it stood on a past day.
    """
    if table.num_rows == 0 or "snapshot_date" not in table.column_names:
        return None
    wrong = pc.not_equal(table.column("snapshot_date"), pa.scalar(day, type=pa.date32()))
    count = pc.sum(pc.cast(wrong, pa.int64())).as_py() or 0
    if not count:
        return None
    return hard(
        "snapshot_date",
        f"{count} row(s) are not stamped {day.isoformat()}",
        samples=_samples(table, wrong, ["ticker", "snapshot_date"], dataset="tickers"),
        count=count,
    )


def check_ticker_snapshot_sanity(table: pa.Table) -> list[Finding]:
    """A ticker snapshot must contain both active and delisted names.

    An all-active snapshot means the ``active`` filter leaked back in, which
    silently reintroduces survivorship bias -- the single thing this dataset
    exists to prevent. Hard, because it cannot happen on a correct request.
    """
    if table.num_rows == 0 or "active" not in table.column_names:
        return []
    active = pc.sum(pc.cast(table.column("active"), pa.int64())).as_py() or 0
    inactive = table.num_rows - active

    findings: list[Finding] = []
    if inactive == 0:
        findings.append(
            hard(
                "ticker_snapshot_sanity",
                f"all {table.num_rows} tickers are active; a survivorship-free "
                "snapshot must include delisted names",
                count=table.num_rows,
            )
        )
    elif active == 0:
        findings.append(
            hard(
                "ticker_snapshot_sanity",
                f"no active tickers among {table.num_rows}",
                count=table.num_rows,
            )
        )
    # A delisted ticker with no delisting date cannot be placed in time.
    if "delisted_utc" in table.column_names and inactive:
        missing = (
            pc.sum(
                pc.cast(
                    pc.and_(
                        pc.invert(table.column("active")),
                        pc.is_null(table.column("delisted_utc")),
                    ),
                    pa.int64(),
                )
            ).as_py()
            or 0
        )
        if missing:
            findings.append(
                soft(
                    "ticker_snapshot_sanity",
                    f"{missing} inactive ticker(s) have no delisting date",
                    count=missing,
                )
            )
    return findings


def check_split_ratios_sane(table: pa.Table) -> Finding | None:
    """Split ratios are positive.

    Fractional ratios are legitimate and common (``split_to`` of
    1.0216095380029806), so this only rejects zero and negative values -- which is
    what an integer cast of such a ratio produces, and the reason the columns are
    floats.
    """
    if table.num_rows == 0 or "split_from" not in table.column_names:
        return None
    bad = pc.or_(
        pc.less_equal(table.column("split_from"), 0.0),
        pc.less_equal(table.column("split_to"), 0.0),
    )
    count = pc.sum(pc.cast(bad, pa.int64())).as_py() or 0
    if not count:
        return None
    return hard(
        "split_ratios_sane",
        f"{count} split(s) with a non-positive ratio",
        samples=_samples(
            table, bad, ["ticker", "execution_date", "split_from", "split_to"], dataset="splits"
        ),
        count=count,
    )


def check_dividend_amounts_sane(table: pa.Table) -> Finding | None:
    """Cash dividend amounts are non-negative."""
    if table.num_rows == 0 or "cash_amount" not in table.column_names:
        return None
    bad = pc.less(table.column("cash_amount"), 0)
    count = pc.sum(pc.cast(bad, pa.int64())).as_py() or 0
    if not count:
        return None
    return hard(
        "dividend_amounts_sane",
        f"{count} dividend(s) with a negative cash amount",
        samples=_samples(
            table, bad, ["ticker", "ex_dividend_date", "cash_amount"], dataset="dividends"
        ),
        count=count,
    )


# --------------------------------------------------------------------------- #
# Completeness
# --------------------------------------------------------------------------- #


def check_is_trading_day(day: date, calendar: TradingCalendar) -> Finding | None:
    """Data for a non-trading day is a hard failure: it should not exist."""
    if calendar.is_trading_day(day):
        return None
    return hard("is_trading_day", f"{day.isoformat()} is not a trading day on {calendar.name}")


def check_eod_ticker_count(
    table: pa.Table,
    *,
    trailing_median: int | None,
    soft_tolerance: float,
    hard_tolerance: float,
) -> Finding | None:
    """EOD ticker count against the trailing median.

    A sharp drop usually means a truncated or partially-served response, which is
    exactly the thing that must never be published as if it were a full day.
    """
    count = table.num_rows
    if not trailing_median:
        return None  # no history yet; nothing to compare against
    deviation = abs(count - trailing_median) / trailing_median
    message = f"{count} tickers vs trailing median {trailing_median} ({deviation:+.1%} deviation)"
    if deviation > hard_tolerance:
        return hard("eod_ticker_count", message, count=count)
    if deviation > soft_tolerance:
        return soft("eod_ticker_count", message, count=count)
    return None


def check_minute_bar_counts(table: pa.Table, day: date, calendar: TradingCalendar) -> list[Finding]:
    """Per-ticker minute bar counts against the day's actual session length.

    Uses the calendar's session length rather than a flat 390, so an early-close
    day (210 bars on 2026-11-27) is judged correctly. Too *many* regular-session
    bars is a hard failure -- it cannot happen on correct data; too few is soft,
    since an illiquid ticker simply has no trades in some minutes.
    """
    if table.num_rows == 0:
        return []
    expected = calendar.regular_bar_count(day)
    if not expected:
        return []

    regular = table.filter(pc.equal(table.column("session"), int(schemas.Session.REGULAR)))
    if regular.num_rows == 0:
        return []
    counts = regular.group_by("ticker").aggregate([("ticker", "count")])
    tickers = counts.column("ticker").to_pylist()
    values = counts.column("ticker_count").to_pylist()

    findings: list[Finding] = []
    over = [f"{t}={n}" for t, n in zip(tickers, values, strict=True) if n > expected]
    if over:
        findings.append(
            hard(
                "minute_bar_counts",
                f"{len(over)} ticker(s) exceed the {expected} regular-session bars "
                f"available on {day.isoformat()}",
                samples=over[:25],
                count=len(over),
            )
        )
    sparse = [f"{t}={n}" for t, n in zip(tickers, values, strict=True) if n < expected * 0.5]
    if sparse:
        findings.append(
            soft(
                "minute_bar_counts",
                f"{len(sparse)} ticker(s) have fewer than half the {expected} expected "
                "regular-session bars",
                samples=sparse[:25],
                count=len(sparse),
            )
        )
    return findings


def check_universe_coverage(
    table: pa.Table, *, universe: list[str], explained: dict[str, str] | None = None
) -> Finding | None:
    """Every ticker in the minute universe has data, or a recorded reason."""
    if not universe:
        return None
    explained = explained or {}
    present = set(table.column("ticker").to_pylist()) if table.num_rows else set()
    missing = [t for t in universe if t not in present and t not in explained]
    if not missing:
        return None
    return soft(
        "universe_coverage",
        f"{len(missing)} universe ticker(s) have no data and no explanation",
        samples=sorted(missing)[:25],
        count=len(missing),
    )


# --------------------------------------------------------------------------- #
# Consistency (soft)
# --------------------------------------------------------------------------- #


def check_large_moves(
    table: pa.Table,
    *,
    previous_closes: dict[str, int],
    split_tickers: set[str] | None = None,
) -> Finding | None:
    """Day-over-day close moves beyond 50%, cross-referenced against splits.

    Never a rejection: real crashes and real squeezes happen. A move with a
    matching split is not reported at all; an unexplained one is flagged.
    """
    if table.num_rows == 0 or not previous_closes:
        return None
    split_tickers = split_tickers or set()
    tickers = table.column("ticker").to_pylist()
    closes = table.column("close").to_pylist()

    unexplained = []
    for ticker, close in zip(tickers, closes, strict=True):
        previous = previous_closes.get(ticker)
        if not previous or not close:
            continue
        # Signed, so a reviewer can tell a crash from a squeeze. Formatting the
        # absolute ratio with "+" made every drop read as a rise.
        change = (close - previous) / previous
        if abs(change) > LARGE_MOVE_RATIO and ticker not in split_tickers:
            unexplained.append(
                f"{ticker} {format_scaled(previous)} -> {format_scaled(close)} ({change:+.0%})"
            )
    if not unexplained:
        return None
    return soft(
        "large_moves",
        f"{len(unexplained)} close move(s) over {LARGE_MOVE_RATIO:.0%} with no matching split",
        samples=unexplained[:25],
        count=len(unexplained),
    )


def check_split_without_discontinuity(
    *, splits: dict[str, float], previous_closes: dict[str, int], closes: dict[str, int]
) -> Finding | None:
    """A split on the books with no matching price discontinuity, or vice versa."""
    suspicious = []
    for ticker, ratio in splits.items():
        previous, current = previous_closes.get(ticker), closes.get(ticker)
        if not previous or not current:
            continue
        expected = previous / ratio
        if expected and abs(current - expected) / expected > 0.25:
            suspicious.append(
                f"{ticker} split 1:{ratio:g} implied {format_scaled(int(expected))} "
                f"but close is {format_scaled(current)}"
            )
    if not suspicious:
        return None
    return soft(
        "split_discontinuity",
        f"{len(suspicious)} split(s) with no matching price discontinuity",
        samples=suspicious[:25],
        count=len(suspicious),
    )


def check_minute_eod_reconciliation(minute: pa.Table, eod: pa.Table, *, day: date) -> list[Finding]:
    """Reconcile minute bars against the EOD bar for the same ticker and day.

    Three comparisons, each against the session scope it actually reconciles in
    (established by measurement -- see the tolerance constants above):

    * **high/low**, regular session only. The EOD bar's high and low come from the
      regular session: all four tested tickers matched to 0.0000%, while including
      pre- and after-hours pushed AAPL's high from 337.085 to 337.81. Tight.
    * **transactions**, all sessions. Reconciles to within 0.7%.
    * **volume**, all sessions, loose. See ``VOLUME_RECONCILE_TOLERANCE``.

    All soft: the provider builds its daily and minute bars from different trade
    sets, so a disagreement is something to look at, never grounds to reject.
    """
    if minute.num_rows == 0 or eod.num_rows == 0:
        return []
    regular = minute.filter(pc.equal(minute.column("session"), int(schemas.Session.REGULAR)))
    if regular.num_rows == 0:
        return []

    regular_agg = {
        row["ticker"]: row
        for row in regular.group_by("ticker")
        .aggregate([("high", "max"), ("low", "min")])
        .to_pylist()
    }
    all_agg = {
        row["ticker"]: row
        for row in minute.group_by("ticker")
        .aggregate([("volume", "sum"), ("transactions", "sum")])
        .to_pylist()
    }
    eod_rows = {row["ticker"]: row for row in eod.to_pylist()}

    price_off: list[str] = []
    txn_off: list[str] = []
    volume_off: list[str] = []

    for ticker, agg in regular_agg.items():
        e = eod_rows.get(ticker)
        if e is None:
            continue
        for field, minute_value, eod_value in (
            ("high", agg["high_max"], e["high"]),
            ("low", agg["low_min"], e["low"]),
        ):
            if eod_value and abs(minute_value - eod_value) / eod_value > PRICE_RECONCILE_TOLERANCE:
                price_off.append(
                    f"{ticker} {field} minute={format_scaled(minute_value)} "
                    f"eod={format_scaled(eod_value)}"
                )

        totals = all_agg.get(ticker, {})
        minute_txns, eod_txns = totals.get("transactions_sum"), e.get("transactions")
        if minute_txns and eod_txns:
            drift = abs(minute_txns - eod_txns) / eod_txns
            if drift > TRANSACTION_RECONCILE_TOLERANCE:
                txn_off.append(f"{ticker} transactions minute={minute_txns} eod={eod_txns}")

        minute_volume, eod_volume = totals.get("volume_sum"), e.get("volume")
        if minute_volume and eod_volume:
            drift = abs(minute_volume - eod_volume) / eod_volume
            if drift > VOLUME_RECONCILE_TOLERANCE:
                volume_off.append(
                    f"{ticker} volume minute={format_scaled(minute_volume)} "
                    f"eod={format_scaled(eod_volume)} ({drift:.0%})"
                )

    checked = len(regular_agg)
    findings: list[Finding] = []
    for check, offenders, tolerance, what in (
        ("minute_eod_price_reconciliation", price_off, PRICE_RECONCILE_TOLERANCE, "high/low"),
        (
            "minute_eod_transaction_reconciliation",
            txn_off,
            TRANSACTION_RECONCILE_TOLERANCE,
            "transaction count",
        ),
        ("minute_eod_volume_reconciliation", volume_off, VOLUME_RECONCILE_TOLERANCE, "volume"),
    ):
        if offenders:
            findings.append(
                soft(
                    check,
                    f"{len(offenders)}/{checked} ticker(s) disagree on {what} beyond "
                    f"{tolerance:.1%} on {day.isoformat()}",
                    samples=offenders[:25],
                    count=len(offenders),
                )
            )
    return findings


# --------------------------------------------------------------------------- #
# Run level (hard)
# --------------------------------------------------------------------------- #


def check_run_failure_ratio(*, failed: int, total: int, abort_ratio: float) -> Finding | None:
    """Stop a run that is mostly failing rather than keep hammering a broken source."""
    if total <= 0:
        return None
    ratio = failed / total
    if ratio <= abort_ratio:
        return None
    return hard(
        "run_failure_ratio",
        f"{failed}/{total} batches failed ({ratio:.0%}), over the {abort_ratio:.0%} abort "
        "threshold; stopping the run",
        count=failed,
    )


__all__ = [
    "LARGE_MOVE_RATIO",
    "REGULAR_FULL_SESSION_BARS",
    "check_dividend_amounts_sane",
    "check_eod_ticker_count",
    "check_is_trading_day",
    "check_large_moves",
    "check_minute_bar_counts",
    "check_minute_eod_reconciliation",
    "check_no_duplicate_keys",
    "check_ohlc_ordering",
    "check_precision_loss",
    "check_prices_positive",
    "check_run_failure_ratio",
    "check_session_labels",
    "check_snapshot_date",
    "check_split_ratios_sane",
    "check_split_without_discontinuity",
    "check_ticker_snapshot_sanity",
    "check_timestamps_in_window",
    "check_universe_coverage",
    "check_volume_non_negative",
    "check_vwap_in_range",
]
