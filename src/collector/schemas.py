"""Registered, versioned Arrow schemas.

A batch is only published if its table matches the registered schema exactly:
column names, order, types and nullability. ``ticker`` is typed as Arrow
``string``; the dictionary encoding the plan calls for is applied by the Parquet
writer (``use_dictionary``) rather than by using Arrow's dictionary type, which
would not survive a read back without reader-side configuration and so would
break exact round-tripping.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

import pyarrow as pa

SCHEMA_VERSION = 2

# Parquet key/value metadata written alongside every published file.
META_SCHEMA_VERSION = b"collector.schema_version"
META_DATASET = b"collector.dataset"
#: Scale of the int64 price/volume columns, so a reader can never guess wrong.
META_SCALE = b"collector.scale"


class Session(IntEnum):
    """Which trading session a minute bar belongs to."""

    PRE_MARKET = 0
    REGULAR = 1
    AFTER_HOURS = 2


# Prices and volumes are both scaled int64 (see collector.money): the provider
# reports fractional share volume, so an integer share count cannot hold it.
_PRICE = pa.int64()
_QTY = pa.int64()
_TS = pa.timestamp("ms", tz="UTC")

EOD_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("date", pa.date32(), nullable=False),
        pa.field("open", _PRICE, nullable=False),
        pa.field("high", _PRICE, nullable=False),
        pa.field("low", _PRICE, nullable=False),
        pa.field("close", _PRICE, nullable=False),
        pa.field("vwap", _PRICE, nullable=True),
        pa.field("volume", _QTY, nullable=False),
        pa.field("transactions", pa.int32(), nullable=True),
        pa.field("ingested_at", _TS, nullable=False),
    ]
)

MINUTE_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("ts", _TS, nullable=False),
        pa.field("open", _PRICE, nullable=False),
        pa.field("high", _PRICE, nullable=False),
        pa.field("low", _PRICE, nullable=False),
        pa.field("close", _PRICE, nullable=False),
        pa.field("vwap", _PRICE, nullable=True),
        pa.field("volume", _QTY, nullable=False),
        pa.field("transactions", pa.int32(), nullable=True),
        pa.field("session", pa.int8(), nullable=False),
        pa.field("ingested_at", _TS, nullable=False),
    ]
)

SPLITS_SCHEMA = pa.schema(
    [
        # The provider's own stable identifier. It is the key, because
        # (ticker, execution_date) is NOT unique: AFOVF has two separate splits
        # executing on 2026-08-05.
        pa.field("id", pa.string(), nullable=False),
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("execution_date", pa.date32(), nullable=False),
        # float64, and the only place floats are used. Split ratios are genuinely
        # fractional -- the provider sends `"split_to": 1.0216095380029806` -- so
        # the int64 the plan assumed would truncate them to 1, and a ratio below 1
        # to 0. These are dimensionless ratios rather than money, float64 is
        # exactly what the provider sends, and copying it verbatim is the most
        # faithful record; it round-trips through Parquet without drift, so
        # determinism and checksums still hold.
        pa.field("split_from", pa.float64(), nullable=False),
        pa.field("split_to", pa.float64(), nullable=False),
        pa.field("ingested_at", _TS, nullable=False),
    ]
)

DIVIDENDS_SCHEMA = pa.schema(
    [
        # Keyed by the provider's id: a ticker can pay two dividends with the same
        # ex-date (JGRRF pays a CD and an SC on 2026-07-06).
        pa.field("id", pa.string(), nullable=False),
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("ex_dividend_date", pa.date32(), nullable=False),
        pa.field("pay_date", pa.date32(), nullable=True),
        pa.field("record_date", pa.date32(), nullable=True),
        pa.field("declaration_date", pa.date32(), nullable=True),
        pa.field("cash_amount", _PRICE, nullable=False),
        # Not every dividend is in USD -- HKD and CHF both appear in live data --
        # so dropping this would make a CHF 2.70 read as USD 2.70.
        pa.field("currency", pa.string(), nullable=True),
        pa.field("frequency", pa.int32(), nullable=True),
        pa.field("dividend_type", pa.string(), nullable=True),
        pa.field("ingested_at", _TS, nullable=False),
    ]
)

TICKERS_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("primary_exchange", pa.string(), nullable=True),
        pa.field("type", pa.string(), nullable=True),
        pa.field("active", pa.bool_(), nullable=False),
        pa.field("list_date", pa.date32(), nullable=True),
        pa.field("delisted_utc", _TS, nullable=True),
        pa.field("cik", pa.string(), nullable=True),
        pa.field("composite_figi", pa.string(), nullable=True),
        pa.field("share_class_figi", pa.string(), nullable=True),
        pa.field("snapshot_date", pa.date32(), nullable=False),
        pa.field("ingested_at", _TS, nullable=False),
    ]
)

UNIVERSE_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("rank", pa.int32(), nullable=False),
        pa.field("dollar_volume", pa.int64(), nullable=False),
        pa.field("snapshot_date", pa.date32(), nullable=False),
        pa.field("ingested_at", _TS, nullable=False),
    ]
)


@dataclass(frozen=True)
class Dataset:
    """A registered dataset: its schema, sort order and partitioning."""

    name: str
    schema: pa.Schema
    # Columns the rows are sorted by before writing. Consecutive rows for the
    # same ticker have similar values, which zstd exploits heavily.
    sort_by: tuple[str, ...]
    # Columns that must be unique together.
    key: tuple[str, ...]
    # Columns written with Parquet dictionary encoding.
    dictionary_columns: tuple[str, ...] = ("ticker",)


EOD = Dataset("eod", EOD_SCHEMA, sort_by=("ticker", "date"), key=("ticker", "date"))
MINUTE = Dataset("minute", MINUTE_SCHEMA, sort_by=("ticker", "ts"), key=("ticker", "ts"))
SPLITS = Dataset(
    "splits",
    SPLITS_SCHEMA,
    sort_by=("ticker", "execution_date", "id"),
    key=("id",),
)
DIVIDENDS = Dataset(
    "dividends",
    DIVIDENDS_SCHEMA,
    sort_by=("ticker", "ex_dividend_date", "id"),
    key=("id",),
)
TICKERS = Dataset("tickers", TICKERS_SCHEMA, sort_by=("ticker",), key=("ticker", "snapshot_date"))
UNIVERSE = Dataset("universe", UNIVERSE_SCHEMA, sort_by=("rank",), key=("ticker", "snapshot_date"))

DATASETS: dict[str, Dataset] = {
    d.name: d for d in (EOD, MINUTE, SPLITS, DIVIDENDS, TICKERS, UNIVERSE)
}

#: Columns holding scaled-integer quantities (prices and volumes), per dataset.
#: Everything listed here is in units of 1/money.SCALE.
SCALED_COLUMNS: dict[str, tuple[str, ...]] = {
    "eod": ("open", "high", "low", "close", "vwap", "volume"),
    "minute": ("open", "high", "low", "close", "vwap", "volume"),
    "splits": (),
    "dividends": ("cash_amount",),
    "tickers": (),
    "universe": ("dollar_volume",),
}

#: The subset that are prices, for the OHLC relationship checks.
PRICE_COLUMNS: dict[str, tuple[str, ...]] = {
    "eod": ("open", "high", "low", "close", "vwap"),
    "minute": ("open", "high", "low", "close", "vwap"),
    "splits": (),
    "dividends": ("cash_amount",),
    "tickers": (),
    "universe": (),
}


def get(name: str) -> Dataset:
    """Look up a registered dataset by name."""
    try:
        return DATASETS[name]
    except KeyError:
        raise KeyError(f"unknown dataset {name!r}; known: {sorted(DATASETS)}") from None


def empty_table(dataset: Dataset | str) -> pa.Table:
    """An empty table with the dataset's exact schema. Useful in tests."""
    ds = get(dataset) if isinstance(dataset, str) else dataset
    return ds.schema.empty_table()
