"""API JSON to Arrow tables.

The provider's field names are terse (``T``, ``o``, ``h``, ``l``, ``c``, ``v``,
``vw``, ``t``, ``n``); these functions map them to the registered schemas,
convert prices and volumes to scaled integers, and report how many values had to
be rounded so the caller can raise a soft flag.

Returned alongside each table is a ``rounded`` map of column name to the number
of values that lost precision. It is almost always empty -- measured against a
full grouped-daily response, nothing rounds at the 1e6 scale -- but it is
reported rather than assumed, because the provider could widen its precision
without telling anyone.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

import pyarrow as pa

from . import schemas
from .checks.trading_calendar import TradingCalendar
from .money import to_scaled_checked

#: Fields the provider omits when it has no value for them.
#: Measured: 3 of 12,605 EOD rows had neither ``vw`` nor ``n``.
OPTIONAL_FIELDS = ("vw", "n")


@dataclass
class Parsed:
    """A table plus the precision-loss counts for it."""

    table: pa.Table
    rounded: dict[str, int] = field(default_factory=dict)
    #: Rows dropped because a required field was missing entirely.
    skipped: list[str] = field(default_factory=list)


class ParseError(ValueError):
    """Raised when a response cannot be mapped to the schema at all."""


def _scaled_column(
    rows: list[dict[str, Any]], key: str, rounded: dict[str, int], name: str, *, required: bool
) -> list[int | None]:
    out: list[int | None] = []
    count = 0
    for row in rows:
        value = row.get(key)
        if value is None:
            if required:
                raise ParseError(f"row missing required field {key!r}: {row}")
            out.append(None)
            continue
        scaled, was_rounded = to_scaled_checked(value)
        out.append(scaled)
        count += was_rounded
    if count:
        rounded[name] = count
    return out


def _ms_to_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def eod(results: list[dict[str, Any]], *, day: date, ingested_at: datetime) -> Parsed:
    """Grouped daily results to the EOD table."""
    rows = [r for r in results if "T" in r]
    rounded: dict[str, int] = {}
    skipped = [str(r) for r in results if "T" not in r]

    table = pa.table(
        {
            "ticker": [r["T"] for r in rows],
            "date": [day] * len(rows),
            "open": _scaled_column(rows, "o", rounded, "open", required=True),
            "high": _scaled_column(rows, "h", rounded, "high", required=True),
            "low": _scaled_column(rows, "l", rounded, "low", required=True),
            "close": _scaled_column(rows, "c", rounded, "close", required=True),
            "vwap": _scaled_column(rows, "vw", rounded, "vwap", required=False),
            "volume": _scaled_column(rows, "v", rounded, "volume", required=True),
            "transactions": [r.get("n") for r in rows],
            "ingested_at": [ingested_at] * len(rows),
        },
        schema=schemas.EOD_SCHEMA,
    )
    return Parsed(table=table, rounded=rounded, skipped=skipped)


def minute(
    results: list[dict[str, Any]],
    *,
    ticker: str,
    day: date,
    calendar: TradingCalendar,
    ingested_at: datetime,
    include_extended_hours: bool = True,
) -> Parsed:
    """Per-ticker aggregate results to the minute table.

    Bars outside the collection window are dropped rather than stored: a range
    request can return bars for neighbouring days, and the batch is published
    under one day's partition.
    """
    rounded: dict[str, int] = {}
    kept: list[dict[str, Any]] = []
    sessions: list[int] = []
    skipped: list[str] = []

    for row in results:
        if "t" not in row:
            skipped.append(f"bar without a timestamp: {row}")
            continue
        ts = _ms_to_datetime(row["t"])
        session = calendar.classify(ts, day)
        if session is None:
            skipped.append(f"{ts.isoformat()} outside the session window")
            continue
        if not include_extended_hours and session is not schemas.Session.REGULAR:
            continue
        kept.append(row)
        sessions.append(int(session))

    table = pa.table(
        {
            "ticker": [ticker] * len(kept),
            "ts": [_ms_to_datetime(r["t"]) for r in kept],
            "open": _scaled_column(kept, "o", rounded, "open", required=True),
            "high": _scaled_column(kept, "h", rounded, "high", required=True),
            "low": _scaled_column(kept, "l", rounded, "low", required=True),
            "close": _scaled_column(kept, "c", rounded, "close", required=True),
            "vwap": _scaled_column(kept, "vw", rounded, "vwap", required=False),
            "volume": _scaled_column(kept, "v", rounded, "volume", required=True),
            "transactions": [r.get("n") for r in kept],
            "session": sessions,
            "ingested_at": [ingested_at] * len(kept),
        },
        schema=schemas.MINUTE_SCHEMA,
    )
    return Parsed(table=table, rounded=rounded, skipped=skipped)


def splits(results: list[dict[str, Any]], *, ingested_at: datetime) -> Parsed:
    """Split records, keyed by the provider's id.

    ``split_from``/``split_to`` are kept as floats. Casting them to int, as the
    plan's schema implied, truncates a real ratio: the provider sends values like
    ``1.0216095380029806``, and anything below 1 would become 0.
    """
    rows = [r for r in results if r.get("id") and r.get("ticker") and r.get("execution_date")]
    skipped = [
        f"split missing id/ticker/execution_date: {r}"
        for r in results
        if not (r.get("id") and r.get("ticker") and r.get("execution_date"))
    ]
    table = pa.table(
        {
            "id": [r["id"] for r in rows],
            "ticker": [r["ticker"] for r in rows],
            "execution_date": [_as_date(r["execution_date"]) for r in rows],
            "split_from": [float(r["split_from"]) for r in rows],
            "split_to": [float(r["split_to"]) for r in rows],
            "ingested_at": [ingested_at] * len(rows),
        },
        schema=schemas.SPLITS_SCHEMA,
    )
    return Parsed(table=table, skipped=skipped)


def dividends(results: list[dict[str, Any]], *, ingested_at: datetime) -> Parsed:
    """Dividend records, keyed by the provider's id.

    ``currency`` is carried through: not every dividend is in USD, and without it
    a CHF 2.70 would be indistinguishable from USD 2.70.
    """
    rows = [r for r in results if r.get("id") and r.get("ticker") and r.get("ex_dividend_date")]
    skipped = [
        f"dividend missing id/ticker/ex_dividend_date: {r}"
        for r in results
        if not (r.get("id") and r.get("ticker") and r.get("ex_dividend_date"))
    ]
    rounded: dict[str, int] = {}
    table = pa.table(
        {
            "id": [r["id"] for r in rows],
            "ticker": [r["ticker"] for r in rows],
            "ex_dividend_date": [_as_date(r["ex_dividend_date"]) for r in rows],
            "pay_date": [_as_date(r.get("pay_date")) for r in rows],
            "record_date": [_as_date(r.get("record_date")) for r in rows],
            "declaration_date": [_as_date(r.get("declaration_date")) for r in rows],
            "cash_amount": _scaled_column(
                rows, "cash_amount", rounded, "cash_amount", required=True
            ),
            "currency": [r.get("currency") for r in rows],
            "frequency": [r.get("frequency") for r in rows],
            "dividend_type": [r.get("dividend_type") for r in rows],
            "ingested_at": [ingested_at] * len(rows),
        },
        schema=schemas.DIVIDENDS_SCHEMA,
    )
    return Parsed(table=table, rounded=rounded, skipped=skipped)


def tickers(results: list[dict[str, Any]], *, snapshot: date, ingested_at: datetime) -> Parsed:
    """Ticker reference snapshot.

    ``active`` is stored as a real boolean and ``delisted_utc`` kept, because
    together they are what lets a reader reconstruct the investable universe as
    it stood on a past date.
    """
    rows = [r for r in results if r.get("ticker")]
    table = pa.table(
        {
            "ticker": [r["ticker"] for r in rows],
            "name": [r.get("name") for r in rows],
            "primary_exchange": [r.get("primary_exchange") for r in rows],
            "type": [r.get("type") for r in rows],
            "active": [bool(r.get("active", False)) for r in rows],
            "list_date": [_as_date(r.get("list_date")) for r in rows],
            "delisted_utc": [_as_datetime(r.get("delisted_utc")) for r in rows],
            "cik": [r.get("cik") for r in rows],
            "composite_figi": [r.get("composite_figi") for r in rows],
            "share_class_figi": [r.get("share_class_figi") for r in rows],
            "snapshot_date": [snapshot] * len(rows),
            "ingested_at": [ingested_at] * len(rows),
        },
        schema=schemas.TICKERS_SCHEMA,
    )
    return Parsed(table=table)


def _as_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _as_datetime(value: str | datetime | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
