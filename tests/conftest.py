from __future__ import annotations

from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from collector import schemas, storage
from collector.money import to_scaled

INGESTED_AT = datetime(2026, 9, 30, 22, 5, 0, tzinfo=UTC)

#: Environment the dev image sets for itself. Left in place it would silently
#: override what a test configures, so every test starts from a clean slate and
#: opts in to whatever it needs.
_AMBIENT_ENV = (
    "COLLECTOR_CONFIG",
    "COLLECTOR_DATA_ROOT",
    "COLLECTOR_LOG_LEVEL",
    "MASSIVE_API_KEY",
    "POLYGON_API_KEY",
    "COLLECTOR_ALERT_WEBHOOK",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in _AMBIENT_ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def paths(tmp_path) -> storage.DataPaths:
    return storage.DataPaths.from_root(tmp_path / "data").ensure()


def eod_rows(
    *,
    day: date = date(2026, 9, 30),
    tickers: tuple[str, ...] = ("AAPL", "MSFT", "ZZZZ"),
    ingested_at: datetime = INGESTED_AT,
) -> dict[str, list]:
    """Hand-built EOD rows with prices chosen to exercise the 4-decimal scale."""
    prices = {
        "AAPL": ("254.1234", "256.9999", "253.0001", "255.5000", "255.1111"),
        "MSFT": ("410.0000", "415.2500", "409.8750", "414.1000", "412.6666"),
        "ZZZZ": ("0.0051", "0.0075", "0.0049", "0.0060", "0.0058"),
    }
    rows: dict[str, list] = {name: [] for name in schemas.EOD_SCHEMA.names}
    for index, ticker in enumerate(tickers):
        o, h, low, c, v = prices[ticker]
        rows["ticker"].append(ticker)
        rows["date"].append(day)
        rows["open"].append(to_scaled(o))
        rows["high"].append(to_scaled(h))
        rows["low"].append(to_scaled(low))
        rows["close"].append(to_scaled(c))
        rows["vwap"].append(to_scaled(v) if ticker != "ZZZZ" else None)
        # Fractional volume, as the provider actually reports it.
        rows["volume"].append(to_scaled(f"{1_000_000 + index}.3651{index}4"))
        rows["transactions"].append(5_000 + index if ticker != "ZZZZ" else None)
        rows["ingested_at"].append(ingested_at)
    return rows


@pytest.fixture
def eod_table() -> pa.Table:
    return pa.table(eod_rows(), schema=schemas.EOD_SCHEMA)


@pytest.fixture
def minute_table() -> pa.Table:
    rows: dict[str, list] = {name: [] for name in schemas.MINUTE_SCHEMA.names}
    base = datetime(2026, 9, 30, 13, 30, tzinfo=UTC)  # 09:30 ET
    for minute in range(5):
        for ticker in ("AAPL", "MSFT"):
            rows["ticker"].append(ticker)
            rows["ts"].append(base.replace(minute=30 + minute))
            rows["open"].append(to_scaled("100.0001"))
            rows["high"].append(to_scaled("100.5000"))
            rows["low"].append(to_scaled("99.9999"))
            rows["close"].append(to_scaled("100.2500"))
            rows["vwap"].append(to_scaled("100.1234"))
            rows["volume"].append(to_scaled(f"{1_000 + minute}.363143"))
            rows["transactions"].append(10 + minute)
            rows["session"].append(int(schemas.Session.REGULAR))
            rows["ingested_at"].append(INGESTED_AT)
    return pa.table(rows, schema=schemas.MINUTE_SCHEMA)
