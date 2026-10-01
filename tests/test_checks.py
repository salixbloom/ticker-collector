"""M2: every hard check has a corrupted fixture that is quarantined; every soft
check has a fixture that publishes with a flag."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest

from collector import checks, schemas, storage
from collector import manifest as manifest_module
from collector.checks import CheckContext, rules
from collector.checks.base import CheckReport, Severity, hard, soft
from collector.checks.trading_calendar import DEFAULT as CAL
from collector.manifest import Manifest
from collector.money import to_scaled

from .conftest import INGESTED_AT, eod_rows

TRADING_DAY = date(2026, 9, 30)  # a Wednesday
HOLIDAY = date(2026, 11, 26)  # Thanksgiving
WEEKEND = date(2026, 10, 3)  # a Saturday
EARLY_CLOSE = date(2026, 11, 27)


@pytest.fixture
def ctx() -> CheckContext:
    return CheckContext(day=TRADING_DAY)


def eod(**overrides) -> pa.Table:
    rows = eod_rows(day=overrides.pop("day", TRADING_DAY))
    for key, values in overrides.items():
        rows[key] = values
    return pa.table(rows, schema=schemas.EOD_SCHEMA)


def minute_table(
    *, day: date = TRADING_DAY, tickers=("AAPL",), minutes: int = 3, session=None, start_hour=13
) -> pa.Table:
    """Minute bars starting at 13:30 UTC = 09:30 ET, the regular open."""
    rows: dict[str, list] = {name: [] for name in schemas.MINUTE_SCHEMA.names}
    open_at = datetime(day.year, day.month, day.day, start_hour, 30, tzinfo=UTC)
    for ticker in tickers:
        for i in range(minutes):
            ts = open_at + timedelta(minutes=i)
            rows["ticker"].append(ticker)
            rows["ts"].append(ts)
            rows["open"].append(to_scaled("100.0"))
            rows["high"].append(to_scaled("100.5"))
            rows["low"].append(to_scaled("99.5"))
            rows["close"].append(to_scaled("100.25"))
            rows["vwap"].append(to_scaled("100.1"))
            rows["volume"].append(to_scaled("1000.363143"))
            rows["transactions"].append(10)
            rows["session"].append(
                int(schemas.Session.REGULAR) if session is None else int(session)
            )
            rows["ingested_at"].append(INGESTED_AT)
    return pa.table(rows, schema=schemas.MINUTE_SCHEMA)


# --------------------------------------------------------------------------- #
# Report mechanics
# --------------------------------------------------------------------------- #


def test_a_clean_eod_batch_passes_every_check(ctx):
    report = checks.run(storage.normalise(eod(), "eod"), "eod", "b1", ctx)
    assert report.ok
    assert report.outcome == "ok"
    assert report.findings == []
    # The report proves what was examined, not just what failed.
    assert {"ohlc_ordering", "prices_positive", "no_duplicate_keys", "is_trading_day"} <= set(
        report.passed
    )


def test_hard_beats_soft_in_the_verdict():
    report = CheckReport("eod", "b")
    report.add(soft("a", "flagged"))
    assert report.ok and report.outcome == "soft_flags"
    report.add(hard("b", "broken"))
    assert not report.ok and report.outcome == "hard_failure"


def test_samples_are_capped_so_a_systemic_failure_stays_small():
    finding = hard("x", "many", samples=[f"row{i}" for i in range(500)], count=500)
    rendered = finding.to_dict(max_samples=10)
    assert len(rendered["samples"]) == 10
    assert rendered["samples_truncated"] == 490
    assert rendered["count"] == 500


# --------------------------------------------------------------------------- #
# HARD checks -- each with a deliberately corrupted fixture
# --------------------------------------------------------------------------- #


def test_hard_non_trading_day_weekend(ctx):
    assert rules.check_is_trading_day(WEEKEND, CAL).is_hard


def test_hard_non_trading_day_holiday():
    """Thanksgiving 2026-11-26 is a holiday, so data for it cannot be real."""
    finding = rules.check_is_trading_day(HOLIDAY, CAL)
    assert finding is not None and finding.is_hard
    assert rules.check_is_trading_day(TRADING_DAY, CAL) is None


def test_hard_duplicate_keys():
    table = storage.normalise(eod(), "eod")
    doubled = storage.normalise(pa.concat_tables([table, table]), "eod")
    finding = rules.check_no_duplicate_keys(doubled, "eod")
    assert finding is not None and finding.is_hard
    assert finding.count == 3


def test_hard_wrong_date_for_the_batch():
    """A row dated differently from the batch means the response was mislabelled."""
    table = storage.normalise(eod(day=date(2026, 9, 29)), "eod")
    finding = rules.check_timestamps_in_window(table, "eod", TRADING_DAY, CAL)
    assert finding is not None and finding.is_hard
    assert finding.count == 3


def test_hard_minute_bar_outside_the_session_window():
    """03:00 UTC is 23:00 ET the night before -- outside any session."""
    table = storage.normalise(minute_table(start_hour=3), "minute")
    finding = rules.check_timestamps_in_window(table, "minute", TRADING_DAY, CAL)
    assert finding is not None and finding.is_hard


def test_minute_bars_inside_the_window_pass():
    table = storage.normalise(minute_table(), "minute")
    assert rules.check_timestamps_in_window(table, "minute", TRADING_DAY, CAL) is None


def test_hard_non_positive_price():
    table = storage.normalise(eod(low=[0, to_scaled("409.875"), to_scaled("0.0049")]), "eod")
    finding = rules.check_prices_positive(table, "eod")
    assert finding is not None and finding.is_hard
    assert finding.count == 1


def test_hard_negative_volume():
    table = storage.normalise(eod(volume=[-1, 100, 200]), "eod")
    finding = rules.check_volume_non_negative(table, "eod")
    assert finding is not None and finding.is_hard


def test_zero_volume_is_allowed():
    """3 of 12,605 real EOD rows had zero volume, so this must not fail."""
    table = storage.normalise(eod(volume=[0, 100, 200]), "eod")
    assert rules.check_volume_non_negative(table, "eod") is None


def test_hard_high_below_low():
    table = storage.normalise(
        eod(high=[to_scaled("1.0"), to_scaled("415.25"), to_scaled("0.0075")]), "eod"
    )
    finding = rules.check_ohlc_ordering(table, "eod")
    assert finding is not None and finding.is_hard
    assert "low <= open/close <= high" in finding.message


def test_hard_close_outside_the_range():
    table = storage.normalise(
        eod(close=[to_scaled("9999.0"), to_scaled("414.1"), to_scaled("0.006")]), "eod"
    )
    finding = rules.check_ohlc_ordering(table, "eod")
    assert finding is not None and finding.is_hard


def test_hard_wrong_session_label():
    """A regular-session bar mislabelled as pre-market."""
    table = storage.normalise(minute_table(session=schemas.Session.PRE_MARKET), "minute")
    finding = rules.check_session_labels(table, TRADING_DAY, CAL)
    assert finding is not None and finding.is_hard


def test_correct_session_labels_pass():
    table = storage.normalise(minute_table(), "minute")
    assert rules.check_session_labels(table, TRADING_DAY, CAL) is None


def test_hard_too_many_regular_session_bars():
    """More bars than the session has minutes cannot happen on correct data."""
    table = storage.normalise(minute_table(minutes=400), "minute")
    findings = rules.check_minute_bar_counts(table, TRADING_DAY, CAL)
    assert any(f.is_hard for f in findings)


def test_hard_eod_ticker_count_collapse():
    """A truncated response must never publish as if it were a full day."""
    table = storage.normalise(eod(), "eod")  # 3 rows
    finding = rules.check_eod_ticker_count(
        table, trailing_median=12_600, soft_tolerance=0.10, hard_tolerance=0.30
    )
    assert finding is not None and finding.is_hard


def test_hard_run_failure_ratio():
    finding = rules.check_run_failure_ratio(failed=5, total=10, abort_ratio=0.20)
    assert finding is not None and finding.is_hard
    assert rules.check_run_failure_ratio(failed=1, total=10, abort_ratio=0.20) is None
    assert rules.check_run_failure_ratio(failed=0, total=0, abort_ratio=0.20) is None


# --------------------------------------------------------------------------- #
# SOFT checks -- each publishes, flagged
# --------------------------------------------------------------------------- #


def test_soft_vwap_outside_range_is_not_a_hard_failure():
    """7.8% of real EOD rows do this; as a hard check it would quarantine the day."""
    table = storage.normalise(eod(vwap=[to_scaled("0.001"), to_scaled("412.6666"), None]), "eod")
    finding = rules.check_vwap_in_range(table, "eod")
    assert finding is not None
    assert finding.severity is Severity.SOFT
    assert "odd-lot" in finding.message


def test_null_vwap_is_not_flagged():
    """3 of 12,605 real rows had no vwap at all."""
    table = storage.normalise(eod(vwap=[None, None, None]), "eod")
    assert rules.check_vwap_in_range(table, "eod") is None


def test_soft_eod_ticker_count_drift():
    table = storage.normalise(eod(), "eod")
    finding = rules.check_eod_ticker_count(
        table, trailing_median=3, soft_tolerance=0.10, hard_tolerance=0.30
    )
    assert finding is None  # exactly on the median

    finding = rules.check_eod_ticker_count(
        table, trailing_median=4, soft_tolerance=0.10, hard_tolerance=0.60
    )
    assert finding is not None and finding.severity is Severity.SOFT


def test_no_ticker_count_check_without_history():
    """A first run has nothing to compare against and must not invent a comparison."""
    table = storage.normalise(eod(), "eod")
    assert (
        rules.check_eod_ticker_count(
            table, trailing_median=None, soft_tolerance=0.1, hard_tolerance=0.3
        )
        is None
    )


def test_soft_sparse_minute_bars():
    table = storage.normalise(minute_table(minutes=3), "minute")
    findings = rules.check_minute_bar_counts(table, TRADING_DAY, CAL)
    assert findings and all(f.severity is Severity.SOFT for f in findings)
    assert "fewer than half" in findings[0].message


def test_early_close_uses_the_days_real_session_length():
    """2026-11-27 closes at 13:00 ET: 210 bars, not 390."""
    assert CAL.regular_bar_count(EARLY_CLOSE) == 210
    assert CAL.regular_bar_count(date(2026, 11, 30)) == 390
    assert CAL.is_early_close(EARLY_CLOSE)
    # 250 bars is fine on a normal day but impossible on an early close.
    table = storage.normalise(minute_table(day=EARLY_CLOSE, minutes=250, start_hour=14), "minute")
    findings = rules.check_minute_bar_counts(table, EARLY_CLOSE, CAL)
    assert any(f.is_hard for f in findings)


def test_soft_universe_ticker_missing():
    table = storage.normalise(minute_table(tickers=("AAPL",)), "minute")
    finding = rules.check_universe_coverage(table, universe=["AAPL", "MSFT", "NVDA"])
    assert finding is not None and finding.severity is Severity.SOFT
    assert finding.count == 2


def test_an_explained_absence_is_not_flagged():
    """A delisted or halted ticker is accounted for, not owed."""
    table = storage.normalise(minute_table(tickers=("AAPL",)), "minute")
    finding = rules.check_universe_coverage(
        table,
        universe=["AAPL", "MSFT"],
        explained={"MSFT": "delisted 2026-09-15 per reference data"},
    )
    assert finding is None


def test_soft_large_move_without_a_split():
    table = storage.normalise(eod(), "eod")
    previous = {"AAPL": to_scaled("100.0"), "MSFT": to_scaled("410.0")}
    finding = rules.check_large_moves(table, previous_closes=previous)
    assert finding is not None and finding.severity is Severity.SOFT
    assert "AAPL" in " ".join(finding.samples)
    assert "MSFT" not in " ".join(finding.samples)  # unchanged, so not flagged


def test_a_large_move_explained_by_a_split_is_not_flagged():
    """Real crashes are published; a split-explained move is not even reported."""
    table = storage.normalise(eod(), "eod")
    previous = {"AAPL": to_scaled("100.0")}
    assert rules.check_large_moves(table, previous_closes=previous, split_tickers={"AAPL"}) is None


def test_soft_split_without_a_price_discontinuity():
    finding = rules.check_split_without_discontinuity(
        splits={"AAPL": 4.0},
        previous_closes={"AAPL": to_scaled("400.0")},
        closes={"AAPL": to_scaled("399.0")},  # no 4:1 drop happened
    )
    assert finding is not None and finding.severity is Severity.SOFT


def test_a_split_with_a_matching_discontinuity_is_not_flagged():
    assert (
        rules.check_split_without_discontinuity(
            splits={"AAPL": 4.0},
            previous_closes={"AAPL": to_scaled("400.0")},
            closes={"AAPL": to_scaled("100.0")},
        )
        is None
    )


def test_soft_precision_loss_is_reported_not_hidden():
    finding = rules.check_precision_loss({"vwap": 2, "volume": 1, "close": 0})
    assert finding is not None and finding.severity is Severity.SOFT
    assert finding.count == 3
    assert "vwap: 2" in finding.samples
    assert rules.check_precision_loss({}) is None
    assert rules.check_precision_loss({"close": 0}) is None


def test_soft_minute_eod_reconciliation_mismatch():
    minute = storage.normalise(minute_table(tickers=("AAPL",), minutes=3), "minute")
    # EOD claims a far higher high and much more volume than the minute bars show.
    eod_table = pa.table(
        {
            "ticker": ["AAPL"],
            "date": [TRADING_DAY],
            "open": [to_scaled("100.0")],
            "high": [to_scaled("500.0")],
            "low": [to_scaled("99.5")],
            "close": [to_scaled("100.25")],
            "vwap": [to_scaled("100.1")],
            "volume": [to_scaled("999999.0")],
            "transactions": [30],
            "ingested_at": [INGESTED_AT],
        },
        schema=schemas.EOD_SCHEMA,
    )
    findings = rules.check_minute_eod_reconciliation(minute, eod_table, day=TRADING_DAY)
    assert findings and all(f.severity is Severity.SOFT for f in findings)
    kinds = {f.check for f in findings}
    assert kinds == {"minute_eod_price_reconciliation", "minute_eod_volume_reconciliation"}


def test_reconciliation_passes_when_they_agree():
    minute = storage.normalise(minute_table(tickers=("AAPL",), minutes=3), "minute")
    eod_table = pa.table(
        {
            "ticker": ["AAPL"],
            "date": [TRADING_DAY],
            "open": [to_scaled("100.0")],
            "high": [to_scaled("100.5")],
            "low": [to_scaled("99.5")],
            "close": [to_scaled("100.25")],
            "vwap": [to_scaled("100.1")],
            "volume": [to_scaled("3001.089429")],  # 3 x 1000.363143
            "transactions": [30],
            "ingested_at": [INGESTED_AT],
        },
        schema=schemas.EOD_SCHEMA,
    )
    assert rules.check_minute_eod_reconciliation(minute, eod_table, day=TRADING_DAY) == []


# --------------------------------------------------------------------------- #
# The gate: results reach the manifest and the right directory
# --------------------------------------------------------------------------- #


@pytest.fixture
def gate(paths):
    with Manifest(paths.manifest_db) as man:
        yield paths, man


def publish(gate, table, dataset="eod", batch_id="b1", context=None, day=TRADING_DAY):
    paths, man = gate
    return checks.validate_and_publish(
        table,
        dataset=dataset,
        batch_id=batch_id,
        paths=paths,
        manifest=man,
        destination=paths.published_path(dataset, day),
        context=context or CheckContext(day=day),
        source="massive:grouped_daily",
        params={"date": day.isoformat(), "adjusted": False},
    )


def test_a_clean_batch_publishes_and_is_recorded(gate):
    paths, man = gate
    result = publish(gate, eod())

    assert result.published
    assert result.published_path == paths.eod_path(TRADING_DAY)
    assert result.report.outcome == "ok"

    row = man.get_batch("b1")
    assert row["status"] == manifest_module.PUBLISHED
    assert row["checksum_sha256"]
    assert row["ingested_at"]
    assert '"outcome": "ok"' in row["check_json"]
    assert man.gaps("eod") == []  # coverage marked ok


def test_a_hard_failure_is_quarantined_and_nothing_is_published(gate):
    paths, man = gate
    corrupt = eod(high=[to_scaled("1.0"), to_scaled("415.25"), to_scaled("0.0075")])
    result = publish(gate, corrupt)

    assert not result.published
    assert list(paths.iter_published()) == []
    assert not paths.eod_path(TRADING_DAY).exists()

    assert result.quarantine_path.exists()
    assert (paths.quarantine / "b1" / "check_report.json").exists()

    row = man.get_batch("b1")
    assert row["status"] == manifest_module.QUARANTINED
    assert "low <= open/close <= high" in row["reason"]
    # The day is still owed, so the next run retries it.
    assert [g.day for g in man.gaps("eod")] == [TRADING_DAY]


def test_a_soft_flag_publishes_and_records_the_flag(gate):
    _, man = gate
    flagged = eod(vwap=[to_scaled("0.001"), to_scaled("412.6666"), None])
    result = publish(gate, flagged)

    assert result.published
    assert result.report.outcome == "soft_flags"
    row = man.get_batch("b1")
    assert row["status"] == manifest_module.PUBLISHED
    assert "vwap" in row["reason"]
    assert '"severity": "soft"' in row["check_json"]
    assert man.gaps("eod") == []  # soft flags do not leave the day owed


def test_data_for_a_non_trading_day_is_quarantined(gate):
    paths, _ = gate
    result = publish(gate, eod(day=WEEKEND), day=WEEKEND)
    assert not result.published
    assert "not a trading day" in result.report.summary()
    assert list(paths.iter_published()) == []


def test_the_quarantined_report_is_readable_json(gate):
    import json

    paths, _ = gate
    publish(gate, eod(volume=[-1, 100, 200]))
    payload = json.loads((paths.quarantine / "b1" / "check_report.json").read_text())
    assert payload["outcome"] == "hard_failure"
    assert payload["hard"][0]["check"] == "volume_non_negative"
    assert payload["dataset"] == "eod"


def test_a_requarantined_batch_does_not_leave_a_stale_published_file(gate):
    """A later bad revision must not be able to replace a good published file."""
    paths, man = gate
    publish(gate, eod(), batch_id="good")
    good_checksum = storage.sha256_file(paths.eod_path(TRADING_DAY))

    publish(gate, eod(volume=[-5, 1, 2]), batch_id="bad")
    assert storage.sha256_file(paths.eod_path(TRADING_DAY)) == good_checksum
    assert man.get_batch("bad")["status"] == manifest_module.QUARANTINED
    assert man.get_batch("good")["status"] == manifest_module.PUBLISHED


def test_revisions_increment_across_publishes(gate):
    _, man = gate
    publish(gate, eod(), batch_id="r1")
    publish(gate, eod(), batch_id="r2")
    assert man.get_batch("r1")["revision"] == 1
    assert man.get_batch("r2")["revision"] == 2
    assert man.latest_published("eod", TRADING_DAY)["batch_id"] == "r2"


def test_precision_flag_flows_from_context_into_the_manifest(gate):
    _, man = gate
    context = CheckContext(day=TRADING_DAY, rounded_by_column={"vwap": 3})
    result = publish(gate, eod(), context=context)
    assert result.published
    assert "rounded" in man.get_batch("b1")["reason"]


def test_large_move_direction_is_signed_in_the_report():
    """A 53% drop must not read as "+53%" -- a reviewer has to see the direction."""
    table = storage.normalise(
        eod(close=[to_scaled("10.0"), to_scaled("414.1"), to_scaled("0.006")]), "eod"
    )
    crash = rules.check_large_moves(table, previous_closes={"AAPL": to_scaled("100.0")})
    assert crash is not None
    assert "-90%" in " ".join(crash.samples)

    squeeze = rules.check_large_moves(table, previous_closes={"AAPL": to_scaled("1.0")})
    assert squeeze is not None
    assert "+900%" in " ".join(squeeze.samples)
