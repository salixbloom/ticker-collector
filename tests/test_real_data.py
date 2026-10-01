"""The checks and parsers run against real recorded API responses.

The fixtures are verbatim responses captured from the live API on 2026-09-29
(a grouped daily for all 12,605 US stocks, and AAPL's full minute day). They are
the regression test for everything the synthetic fixtures cannot tell us: that
the parsers handle the provider's real field shapes, that no hard check fires on
good data, and that the soft checks fire at the rates actually observed.

A hard check that fires here would quarantine real production days.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.compute as pc
import pytest

from collector import checks, parse, schemas, storage
from collector.checks import CheckContext
from collector.checks.trading_calendar import DEFAULT as CAL
from collector.manifest import Manifest
from collector.money import from_scaled

FIXTURES = Path(__file__).parent / "fixtures"
DAY = date(2026, 9, 29)
INGESTED_AT = datetime(2026, 9, 30, 2, 0, tzinfo=UTC)

pytestmark = pytest.mark.skipif(
    not (FIXTURES / "grouped_2026-09-29.json").exists(),
    reason="recorded API fixtures not present",
)


@pytest.fixture(scope="module")
def grouped() -> list[dict]:
    return json.loads((FIXTURES / "grouped_2026-09-29.json").read_text())["results"]


#: Recorded minute days, chosen to span liquidity and price levels: two megacaps,
#: a high-volume low-priced name, and a mid-cap.
MINUTE_FIXTURES = {
    "AAPL": "minute_aapl_2026-09-29.json",
    "MSFT": "minute_MSFT.json",
    "F": "minute_F.json",
    "SOFI": "minute_SOFI.json",
}


def _minute_results(ticker: str) -> list[dict]:
    return json.loads((FIXTURES / MINUTE_FIXTURES[ticker]).read_text())["results"]


@pytest.fixture(scope="module")
def minute_raw() -> list[dict]:
    return _minute_results("AAPL")


@pytest.fixture(scope="module")
def eod_table(grouped):
    return storage.normalise(parse.eod(grouped, day=DAY, ingested_at=INGESTED_AT).table, "eod")


@pytest.fixture(scope="module")
def minute_table(minute_raw):
    parsed = parse.minute(minute_raw, ticker="AAPL", day=DAY, calendar=CAL, ingested_at=INGESTED_AT)
    return storage.normalise(parsed.table, "minute")


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #


def test_eod_parses_every_row(grouped, eod_table):
    assert eod_table.num_rows == len(grouped) == 12_605
    storage.assert_schema(eod_table, "eod")


def test_nothing_rounds_at_the_1e6_scale(grouped):
    """The reason the scale is 1e6: at 1e4 this day would round 8,657 values."""
    parsed = parse.eod(grouped, day=DAY, ingested_at=INGESTED_AT)
    assert parsed.rounded == {}
    assert parsed.skipped == []


def test_fractional_volume_survives_parsing(eod_table):
    volumes = eod_table.column("volume").to_pylist()
    fractional = [v for v in volumes if v % 1_000_000]
    # 92% of rows had fractional volume in this response.
    assert len(fractional) / len(volumes) > 0.9


def test_a_known_row_round_trips_exactly(eod_table):
    """VGSR, the first row of the raw response."""
    index = eod_table.column("ticker").to_pylist().index("VGSR")
    row = {name: eod_table.column(name)[index].as_py() for name in eod_table.column_names}
    assert from_scaled(row["open"]) == Decimal("10.7")
    assert from_scaled(row["close"]) == Decimal("10.67")
    assert from_scaled(row["high"]) == Decimal("10.7399")
    assert from_scaled(row["low"]) == Decimal("10.6146")
    assert from_scaled(row["vwap"]) == Decimal("10.6844")
    assert from_scaled(row["volume"]) == Decimal("85227.9653")
    assert row["transactions"] == 294
    assert row["date"] == DAY


def test_missing_vwap_and_transactions_become_nulls(eod_table):
    """3 of 12,605 rows had neither field."""
    assert eod_table.column("vwap").null_count == 3
    assert eod_table.column("transactions").null_count == 3


def test_minute_parses_and_classifies_sessions(minute_raw, minute_table):
    assert minute_table.num_rows == len(minute_raw) == 871
    storage.assert_schema(minute_table, "minute")

    sessions = minute_table.column("session").to_pylist()
    counts = {s: sessions.count(s) for s in set(sessions)}
    # A full extended day: pre-market, regular and after-hours all present.
    assert set(counts) == {
        int(schemas.Session.PRE_MARKET),
        int(schemas.Session.REGULAR),
        int(schemas.Session.AFTER_HOURS),
    }
    # The regular session cannot exceed its 390 minutes.
    assert counts[int(schemas.Session.REGULAR)] <= 390


def test_minute_bars_are_all_inside_the_collection_window(minute_table):
    start, end = CAL.extended_window(DAY)
    timestamps = minute_table.column("ts").to_pylist()
    assert min(timestamps) >= start
    assert max(timestamps) < end


# --------------------------------------------------------------------------- #
# No hard check fires on real, good data
# --------------------------------------------------------------------------- #


def test_no_hard_check_fires_on_a_real_eod_day(eod_table):
    report = checks.run(eod_table, "eod", "real-eod", CheckContext(day=DAY))
    assert report.hard_failures == [], [f.message for f in report.hard_failures]
    assert report.ok


def test_no_hard_check_fires_on_a_real_minute_day(minute_table):
    report = checks.run(minute_table, "minute", "real-minute", CheckContext(day=DAY))
    assert report.hard_failures == [], [f.message for f in report.hard_failures]
    assert report.ok


def test_a_real_day_publishes_end_to_end(eod_table, paths):
    with Manifest(paths.manifest_db) as man:
        result = checks.validate_and_publish(
            eod_table,
            dataset="eod",
            batch_id="real-eod",
            paths=paths,
            manifest=man,
            destination=paths.eod_path(DAY),
            context=CheckContext(day=DAY),
            source="massive:grouped_daily",
            params={"date": DAY.isoformat(), "adjusted": False, "include_otc": False},
        )
        assert result.published, result.report.summary()
        reloaded = storage.read_parquet(result.published_path)
        assert reloaded.num_rows == 12_605
        assert man.get_batch("real-eod")["rows"] == 12_605


def test_a_real_day_compresses_to_a_sensible_size(eod_table, paths):
    """Sanity-check the plan's "tens of MB per year" EOD estimate."""
    staged = storage.stage(
        eod_table, dataset="eod", paths=paths, batch_id="size", filename="eod.parquet"
    )
    per_row = staged.bytes / staged.rows
    # ~252 trading days a year at this size.
    yearly_mb = staged.bytes * 252 / 1e6
    assert per_row < 60, f"{per_row:.1f} bytes/row is larger than expected"
    assert yearly_mb < 200, f"{yearly_mb:.0f} MB/year exceeds the plan's estimate"


# --------------------------------------------------------------------------- #
# Soft checks fire at the observed rates
# --------------------------------------------------------------------------- #


def test_vwap_out_of_range_is_flagged_softly_at_the_observed_rate(eod_table):
    """977 of 12,605 rows, which is why this check is soft."""
    finding = checks.rules.check_vwap_in_range(eod_table, "eod")
    assert finding is not None
    assert not finding.is_hard
    assert finding.count == 977


def test_zero_volume_rows_exist_and_do_not_fail(eod_table):
    zero = [v for v in eod_table.column("volume").to_pylist() if v == 0]
    assert len(zero) == 3
    assert checks.rules.check_volume_non_negative(eod_table, "eod") is None


def test_real_ticker_count_matches_a_realistic_median(eod_table):
    """12,605 against a 12,600 median is well inside tolerance."""
    assert (
        checks.rules.check_eod_ticker_count(
            eod_table, trailing_median=12_600, soft_tolerance=0.10, hard_tolerance=0.30
        )
        is None
    )


@pytest.mark.parametrize("ticker", ["AAPL", "MSFT", "F", "SOFI"])
def test_minute_eod_reconciliation_on_real_tickers(ticker, eod_table):
    """The provider's own minute and EOD bars for the same ticker and day.

    Tolerances were set from exactly this measurement, so the assertion is that
    nothing fires: high/low reconcile exactly, transactions within 3%, and volume
    within the deliberately loose 50% tripwire.
    """
    parsed = parse.minute(
        _minute_results(ticker), ticker=ticker, day=DAY, calendar=CAL, ingested_at=INGESTED_AT
    )
    minute = storage.normalise(parsed.table, "minute")
    one_eod = eod_table.filter(pc.equal(eod_table.column("ticker"), ticker))
    assert one_eod.num_rows == 1

    findings = checks.rules.check_minute_eod_reconciliation(minute, one_eod, day=DAY)
    assert findings == [], [f.message for f in findings]


@pytest.mark.parametrize("ticker", ["AAPL", "MSFT", "F", "SOFI"])
def test_eod_high_low_comes_from_the_regular_session(ticker, eod_table):
    """Including extended hours would overstate the high: AAPL 337.085 -> 337.81."""
    parsed = parse.minute(
        _minute_results(ticker), ticker=ticker, day=DAY, calendar=CAL, ingested_at=INGESTED_AT
    )
    minute = storage.normalise(parsed.table, "minute")
    regular = minute.filter(pc.equal(minute.column("session"), int(schemas.Session.REGULAR)))
    one_eod = eod_table.filter(pc.equal(eod_table.column("ticker"), ticker))

    assert pc.max(regular.column("high")).as_py() == one_eod.column("high")[0].as_py()
    assert pc.min(regular.column("low")).as_py() == one_eod.column("low")[0].as_py()


def test_volume_does_not_reconcile_which_is_why_the_check_is_loose(eod_table):
    """Documents the measured disagreement, so a future tightening fails loudly."""
    worst = 0.0
    for ticker in ("AAPL", "MSFT", "F", "SOFI"):
        parsed = parse.minute(
            _minute_results(ticker),
            ticker=ticker,
            day=DAY,
            calendar=CAL,
            ingested_at=INGESTED_AT,
        )
        minute_volume = pc.sum(parsed.table.column("volume")).as_py()
        one_eod = eod_table.filter(pc.equal(eod_table.column("ticker"), ticker))
        eod_volume = one_eod.column("volume")[0].as_py()
        worst = max(worst, abs(minute_volume - eod_volume) / eod_volume)
    # Measured 4.6%-24.9%: far too wide for the 2% the plan assumed.
    assert worst > 0.04
    assert worst < checks.rules.VOLUME_RECONCILE_TOLERANCE


# --------------------------------------------------------------------------- #
# Real corporate actions
# --------------------------------------------------------------------------- #

SPLITS_FIXTURE = FIXTURES / "splits_live.json"
DIVIDENDS_FIXTURE = FIXTURES / "dividends_live.json"

needs_corpactions = pytest.mark.skipif(
    not SPLITS_FIXTURE.exists(), reason="recorded corporate-action fixtures not present"
)


@pytest.fixture(scope="module")
def split_rows() -> list[dict]:
    return json.loads(SPLITS_FIXTURE.read_text())["results"]


@pytest.fixture(scope="module")
def dividend_rows() -> list[dict]:
    return json.loads(DIVIDENDS_FIXTURE.read_text())["results"]


@needs_corpactions
def test_real_splits_parse_and_pass_their_checks(split_rows):
    parsed = parse.splits(split_rows, ingested_at=INGESTED_AT)
    table = storage.normalise(parsed.table, "splits")
    assert table.num_rows == len(split_rows) == 518
    assert parsed.skipped == []

    report = checks.run(table, "splits", "real-splits", CheckContext(day=DAY))
    assert report.hard_failures == [], [f.message for f in report.hard_failures]


@needs_corpactions
def test_real_splits_contain_fractional_ratios(split_rows):
    """The reason split_to is float64: int() would truncate these to 1."""
    table = parse.splits(split_rows, ingested_at=INGESTED_AT).table
    ratios = table.column("split_to").to_pylist()
    fractional = [r for r in ratios if r != int(r)]
    assert fractional, "expected fractional split ratios in live data"
    assert all(r > 0 for r in ratios)
    # And none would survive an integer cast.
    assert any(int(r) != r for r in fractional)


@needs_corpactions
def test_real_splits_are_not_unique_by_ticker_and_date(split_rows):
    """Why the key is the provider's id: AFOVF has two splits on 2026-08-05."""
    pairs = [(r["ticker"], r["execution_date"]) for r in split_rows]
    assert len(set(pairs)) < len(pairs)
    assert len({r["id"] for r in split_rows}) == len(split_rows)

    table = storage.normalise(parse.splits(split_rows, ingested_at=INGESTED_AT).table, "splits")
    assert checks.rules.check_no_duplicate_keys(table, "splits") is None
    assert table.num_rows == len(split_rows)


@needs_corpactions
def test_real_dividends_parse_and_pass_their_checks(dividend_rows):
    parsed = parse.dividends(dividend_rows, ingested_at=INGESTED_AT)
    table = storage.normalise(parsed.table, "dividends")
    assert table.num_rows == len(dividend_rows)
    assert parsed.skipped == []

    report = checks.run(table, "dividends", "real-dividends", CheckContext(day=DAY))
    assert report.hard_failures == [], [f.message for f in report.hard_failures]


@needs_corpactions
def test_real_dividends_are_not_unique_by_ticker_and_ex_date(dividend_rows):
    pairs = [(r["ticker"], r["ex_dividend_date"]) for r in dividend_rows]
    assert len(set(pairs)) < len(pairs)

    table = storage.normalise(
        parse.dividends(dividend_rows, ingested_at=INGESTED_AT).table, "dividends"
    )
    assert checks.rules.check_no_duplicate_keys(table, "dividends") is None
    assert table.num_rows == len(dividend_rows)


@needs_corpactions
def test_real_dividends_include_non_usd_currencies(dividend_rows):
    """Dropping currency would make a CHF amount read as USD."""
    table = parse.dividends(dividend_rows, ingested_at=INGESTED_AT).table
    currencies = set(table.column("currency").to_pylist())
    assert "USD" in currencies
    assert currencies - {"USD", None}, f"expected non-USD dividends, got {currencies}"


@needs_corpactions
def test_real_corporate_actions_round_trip(split_rows, dividend_rows, paths):
    for dataset, rows, parser in (
        ("splits", split_rows, parse.splits),
        ("dividends", dividend_rows, parse.dividends),
    ):
        table = storage.normalise(parser(rows, ingested_at=INGESTED_AT).table, dataset)
        staged = storage.stage(
            table, dataset=dataset, paths=paths, batch_id=dataset, filename=f"{dataset}.parquet"
        )
        reloaded = storage.read_parquet(staged.path)
        assert reloaded.equals(table, check_metadata=False)
        assert storage.content_sha256(reloaded, dataset) == storage.content_sha256(table, dataset)
