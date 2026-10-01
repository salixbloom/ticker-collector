"""M5: ticker snapshots accumulate as point-in-time records; split cross-check runs."""

from __future__ import annotations

from datetime import UTC, date, datetime

import httpx
import pyarrow as pa
import pytest

from collector import checks, jobs, parse, schemas, storage
from collector import manifest as manifest_module
from collector.checks import CheckContext, rules
from collector.checks.trading_calendar import TradingCalendar
from collector.config import Config, SourceConfig
from collector.manifest import Manifest
from collector.money import to_scaled
from collector.ratelimit import RateLimiter
from collector.source import Client

DAY = date(2026, 9, 29)  # a Tuesday
NEXT_DAY = date(2026, 9, 30)
INGESTED_AT = datetime(2026, 9, 30, 2, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def api_key(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-test-ref-123456")


# Shapes taken from the provider's documented reference payloads.
TICKER_ROWS = [
    {
        "ticker": "AAPL",
        "name": "Apple Inc.",
        "market": "stocks",
        "primary_exchange": "XNAS",
        "type": "CS",
        "active": True,
        "list_date": "1980-12-12",
        "cik": "0000320193",
        "composite_figi": "BBG000B9XRY4",
        "share_class_figi": "BBG001S5N8V8",
    },
    {
        "ticker": "DEAD",
        "name": "Delisted Co",
        "market": "stocks",
        "primary_exchange": "XNYS",
        "type": "CS",
        "active": False,
        "list_date": "2015-03-02",
        "delisted_utc": "2024-06-14T00:00:00Z",
    },
    {
        "ticker": "GONE",
        "name": "No Date Co",
        "market": "stocks",
        "type": "CS",
        "active": False,
    },
]

SPLIT_ROWS = [
    {
        "id": "Esplit1",
        "ticker": "NVDA",
        "execution_date": "2026-09-29",
        "split_from": 1,
        "split_to": 10,
    },
    {
        "id": "Esplit2",
        "ticker": "DPU",
        "execution_date": "2026-12-17",
        "split_from": 50,
        "split_to": 1,
    },
    # Two splits for one ticker on one date, and fractional ratios: both real
    # shapes from live data that (ticker, execution_date) keying cannot hold.
    {
        "id": "Esplit3",
        "ticker": "AFOVF",
        "execution_date": "2026-08-05",
        "split_from": 1,
        "split_to": 1.0216095380029806,
    },
    {
        "id": "Esplit4",
        "ticker": "AFOVF",
        "execution_date": "2026-08-05",
        "split_from": 1,
        "split_to": 1.003725782414307,
    },
]

DIVIDEND_ROWS = [
    {
        "id": "Ediv1",
        "ticker": "AAPL",
        "ex_dividend_date": "2026-09-14",
        "pay_date": "2026-09-21",
        "record_date": "2026-09-15",
        "declaration_date": "2026-08-01",
        "cash_amount": 0.26,
        "currency": "USD",
        "frequency": 4,
        "dividend_type": "CD",
    },
    {
        "id": "Ediv2",
        "ticker": "KO",
        "ex_dividend_date": "2026-09-16",
        "cash_amount": 0.485,
        "currency": "USD",
        "frequency": 4,
        "dividend_type": "CD",
    },
    # Same ticker and ex-date, two different dividends -- a real shape (JGRRF) --
    # and non-USD, which is why currency is stored.
    {
        "id": "Ediv3",
        "ticker": "JGRRF",
        "ex_dividend_date": "2026-07-06",
        "cash_amount": 0.002,
        "currency": "HKD",
        "frequency": 0,
        "dividend_type": "SC",
    },
    {
        "id": "Ediv4",
        "ticker": "JGRRF",
        "ex_dividend_date": "2026-07-06",
        "cash_amount": 0.002,
        "currency": "HKD",
        "frequency": 1,
        "dividend_type": "CD",
    },
]


class RefApi:
    """Serves the reference endpoints, with optional pagination and failures."""

    def __init__(self, *, pages: int = 1, fail: set[str] | None = None) -> None:
        self.pages = pages
        self.fail = fail or set()
        self.requested: list[str] = []
        self._page_counts: dict[str | None, int] = {}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.requested.append(path)
        for name in ("tickers", "splits", "dividends"):
            if name in path and name in self.fail:
                return httpx.Response(500)

        if "tickers" in path:
            # Mirror the live API: `active` filters, and there is no value that
            # returns both. The client must make two passes.
            wanted = request.url.params.get("active")
            self._page_counts.setdefault(wanted, 0)
            self._page_counts[wanted] += 1
            seen = self._page_counts[wanted]

            pool = [r for r in TICKER_ROWS if str(r.get("active", False)).lower() == wanted]
            # Distinct tickers per page, as real pagination gives. Identical pages
            # would trip the duplicate-key check rather than exercise paging.
            suffix = "" if seen == 1 else str(seen)
            rows = [dict(r, ticker=r["ticker"] + suffix) for r in pool]
            payload: dict = {"status": "OK", "results": rows}
            if seen < self.pages:
                payload["next_url"] = (
                    f"https://api.massive.com/v3/reference/tickers?active={wanted}&cursor=p{seen}"
                )
            return httpx.Response(200, json=payload)
        if "splits" in path:
            return httpx.Response(200, json={"status": "OK", "results": SPLIT_ROWS})
        if "dividends" in path:
            return httpx.Response(200, json={"status": "OK", "results": DIVIDEND_ROWS})
        return httpx.Response(404)


def make_context(tmp_path, api) -> jobs.Context:
    config = Config(
        data_root=tmp_path / "data",
        source=SourceConfig(base_url="https://api.massive.com", calls_per_minute=10_000),
    )
    paths = storage.DataPaths.from_root(config.data_root).ensure()
    client = Client(
        config=config,
        limiter=RateLimiter(10_000, sleep=lambda _s: None),
        http=httpx.Client(transport=httpx.MockTransport(api), base_url=config.source.base_url),
        sleep=lambda _s: None,
        jitter=lambda: 0.5,
    )
    return jobs.Context(
        config=config,
        client=client,
        manifest=Manifest(paths.manifest_db),
        paths=paths,
        calendar=TradingCalendar(),
    )


@pytest.fixture
def ctx(tmp_path):
    api = RefApi()
    context = make_context(tmp_path, api)
    context.api = api  # type: ignore[attr-defined]
    yield context
    context.client.close()
    context.manifest.close()


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #


def test_tickers_parse_with_active_flag_and_delisting_date():
    table = parse.tickers(TICKER_ROWS, snapshot=DAY, ingested_at=INGESTED_AT).table
    storage.assert_schema(table, "tickers")
    rows = {r["ticker"]: r for r in table.to_pylist()}

    assert rows["AAPL"]["active"] is True
    assert rows["AAPL"]["list_date"] == date(1980, 12, 12)
    assert rows["AAPL"]["delisted_utc"] is None
    assert rows["AAPL"]["cik"] == "0000320193"

    assert rows["DEAD"]["active"] is False
    assert rows["DEAD"]["delisted_utc"] == datetime(2024, 6, 14, tzinfo=UTC)
    assert all(r["snapshot_date"] == DAY for r in rows.values())


def test_splits_parse_keyed_by_provider_id():
    table = parse.splits(SPLIT_ROWS, ingested_at=INGESTED_AT).table
    storage.assert_schema(table, "splits")
    rows = {r["id"]: r for r in table.to_pylist()}

    assert rows["Esplit1"]["ticker"] == "NVDA"
    assert rows["Esplit1"]["split_to"] == 10.0
    assert rows["Esplit1"]["execution_date"] == date(2026, 9, 29)
    # A reverse split, which the ratio must represent without losing direction.
    assert (rows["Esplit2"]["split_from"], rows["Esplit2"]["split_to"]) == (50.0, 1.0)


def test_fractional_split_ratios_survive_exactly():
    """int() would truncate 1.0216095380029806 to 1, and anything under 1 to 0."""
    table = parse.splits(SPLIT_ROWS, ingested_at=INGESTED_AT).table
    rows = {r["id"]: r for r in table.to_pylist()}
    assert rows["Esplit3"]["split_to"] == 1.0216095380029806
    assert rows["Esplit4"]["split_to"] == 1.003725782414307


def test_two_splits_for_one_ticker_on_one_date_both_survive():
    """(ticker, execution_date) is not unique -- AFOVF on 2026-08-05 proves it."""
    table = storage.normalise(parse.splits(SPLIT_ROWS, ingested_at=INGESTED_AT).table, "splits")
    afovf = [r for r in table.to_pylist() if r["ticker"] == "AFOVF"]
    assert len(afovf) == 2
    assert rules.check_no_duplicate_keys(table, "splits") is None


def test_dividends_parse_with_scaled_amounts_and_optional_dates():
    parsed = parse.dividends(DIVIDEND_ROWS, ingested_at=INGESTED_AT)
    storage.assert_schema(parsed.table, "dividends")
    rows = {r["id"]: r for r in parsed.table.to_pylist()}

    assert rows["Ediv1"]["cash_amount"] == to_scaled("0.26")
    assert rows["Ediv1"]["pay_date"] == date(2026, 9, 21)
    assert rows["Ediv2"]["cash_amount"] == to_scaled("0.485")
    assert rows["Ediv2"]["pay_date"] is None  # absent in the payload
    assert parsed.rounded == {}


def test_dividend_currency_is_kept():
    """Without it a CHF 2.70 reads as USD 2.70."""
    table = parse.dividends(DIVIDEND_ROWS, ingested_at=INGESTED_AT).table
    rows = {r["id"]: r for r in table.to_pylist()}
    assert rows["Ediv1"]["currency"] == "USD"
    assert rows["Ediv3"]["currency"] == "HKD"


def test_two_dividends_with_the_same_ex_date_both_survive():
    """JGRRF pays a CD and an SC on 2026-07-06."""
    table = storage.normalise(
        parse.dividends(DIVIDEND_ROWS, ingested_at=INGESTED_AT).table, "dividends"
    )
    jgrrf = [r for r in table.to_pylist() if r["ticker"] == "JGRRF"]
    assert len(jgrrf) == 2
    assert {r["dividend_type"] for r in jgrrf} == {"CD", "SC"}
    assert rules.check_no_duplicate_keys(table, "dividends") is None


# --------------------------------------------------------------------------- #
# Reference job
# --------------------------------------------------------------------------- #


def test_reference_publishes_a_dated_snapshot(ctx):
    result = jobs.run_reference(ctx, today=DAY)
    assert result.published == 1
    assert result.rows == 3

    path = ctx.paths.tickers_path(DAY)
    assert path.exists()
    assert path.name == "tickers_2026-09-29.parquet"
    table = storage.read_parquet(path)
    assert set(table.column("ticker").to_pylist()) == {"AAPL", "DEAD", "GONE"}


def test_snapshots_accumulate_as_separate_point_in_time_files(ctx):
    """M5's definition of done: snapshots accumulate, never overwrite."""
    jobs.run_reference(ctx, today=DAY)
    jobs.run_reference(ctx, today=NEXT_DAY)

    files = sorted(p.name for p in ctx.paths.iter_published("reference"))
    assert files == ["tickers_2026-09-29.parquet", "tickers_2026-09-30.parquet"]
    # Each file is stamped with its own date, so a reader can pick a date.
    for day in (DAY, NEXT_DAY):
        table = storage.read_parquet(ctx.paths.tickers_path(day))
        assert set(table.column("snapshot_date").to_pylist()) == {day}


def test_running_reference_twice_in_a_day_does_not_refetch(ctx):
    jobs.run_reference(ctx, today=DAY)
    calls = len(ctx.api.requested)
    second = jobs.run_reference(ctx, today=DAY)
    assert second.published == 0
    assert len(ctx.api.requested) == calls


def test_reference_does_nothing_on_a_non_trading_day(ctx):
    result = jobs.run_reference(ctx, today=date(2026, 10, 3))  # Saturday
    assert result.published == 0
    assert ctx.api.requested == []
    assert result.outcome == "ok"


def test_a_missed_past_snapshot_is_retired_not_retried_forever(ctx):
    """Reference data is only served as of today, so a missed day is unfillable."""
    missed = date(2026, 9, 28)
    ctx.manifest.expect("tickers", [missed])
    assert [g.day for g in ctx.manifest.gaps("tickers")] == [missed]

    jobs.run_reference(ctx, today=DAY)

    assert ctx.manifest.gaps("tickers") == []  # no longer owed
    summary = ctx.manifest.coverage_summary("tickers")
    assert summary[manifest_module.SKIPPED] == 1
    assert summary[manifest_module.OK] == 1


def test_a_reference_fetch_failure_leaves_the_day_owed(tmp_path):
    api = RefApi(fail={"tickers"})
    ctx = make_context(tmp_path, api)
    try:
        result = jobs.run_reference(ctx, today=DAY)
        assert result.errors
        assert result.outcome == "hard_failure"
        assert [g.day for g in ctx.manifest.gaps("tickers")] == [DAY]
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_reference_paginates_to_the_end(tmp_path):
    api = RefApi(pages=3)
    ctx = make_context(tmp_path, api)
    try:
        result = jobs.run_reference(ctx, today=DAY)
        assert result.published == 1, result.quarantine_reasons
        # 3 pages for each of the active and inactive passes.
        assert len([p for p in api.requested if "tickers" in p]) == 6
        assert result.rows == 9  # 3 pages x (1 active + 2 inactive)
        table = storage.read_parquet(ctx.paths.tickers_path(DAY))
        assert table.num_rows == 9
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_reference_makes_a_separate_pass_for_inactive_tickers(ctx):
    """Omitting `active` returns only active names -- verified against the live API.

    Relying on that default published a snapshot of 13,248 tickers with zero
    delisted names, which is survivorship bias by construction.
    """
    jobs.run_reference(ctx, today=DAY)
    asked = [p for p in ctx.api.requested if "tickers" in p]
    assert len(asked) == 2
    assert ctx.api._page_counts == {"true": 1, "false": 1}

    table = storage.read_parquet(ctx.paths.tickers_path(DAY))
    actives = table.column("active").to_pylist()
    assert any(actives) and not all(actives)


def test_duplicate_tickers_across_pages_are_a_hard_failure(tmp_path):
    """A paging bug that re-serves a page must not double the snapshot."""
    duplicated = parse.tickers(
        TICKER_ROWS + TICKER_ROWS, snapshot=DAY, ingested_at=INGESTED_AT
    ).table
    finding = rules.check_no_duplicate_keys(storage.normalise(duplicated, "tickers"), "tickers")
    assert finding is not None and finding.is_hard


# --------------------------------------------------------------------------- #
# Survivorship: the reason this dataset exists
# --------------------------------------------------------------------------- #


def test_a_snapshot_of_only_active_tickers_is_a_hard_failure(ctx):
    """Would silently reintroduce survivorship bias."""
    only_active = [r for r in TICKER_ROWS if r["active"]]
    table = parse.tickers(only_active, snapshot=DAY, ingested_at=INGESTED_AT).table
    findings = rules.check_ticker_snapshot_sanity(storage.normalise(table, "tickers"))
    assert any(f.is_hard for f in findings)
    assert "delisted" in findings[0].message


def test_a_mixed_snapshot_passes_with_a_soft_flag_for_the_missing_date(ctx):
    table = storage.normalise(
        parse.tickers(TICKER_ROWS, snapshot=DAY, ingested_at=INGESTED_AT).table, "tickers"
    )
    findings = rules.check_ticker_snapshot_sanity(table)
    assert not any(f.is_hard for f in findings)
    # GONE is inactive with no delisting date.
    assert findings and findings[0].count == 1


def test_a_snapshot_stamped_with_the_wrong_date_is_rejected(ctx):
    table = storage.normalise(
        parse.tickers(TICKER_ROWS, snapshot=DAY, ingested_at=INGESTED_AT).table, "tickers"
    )
    finding = rules.check_snapshot_date(table, NEXT_DAY)
    assert finding is not None and finding.is_hard


def test_delisted_tickers_reach_the_published_snapshot(ctx):
    """M4's outstanding criterion: delistings are marked in reference data."""
    jobs.run_reference(ctx, today=DAY)
    table = storage.read_parquet(ctx.paths.tickers_path(DAY))
    rows = {r["ticker"]: r for r in table.to_pylist()}
    assert rows["DEAD"]["active"] is False
    assert rows["DEAD"]["delisted_utc"] is not None
    assert rows["AAPL"]["active"] is True


# --------------------------------------------------------------------------- #
# Corporate actions
# --------------------------------------------------------------------------- #


def test_corporate_actions_publishes_both_tables(ctx):
    result = jobs.run_corporate_actions(ctx, today=DAY)
    assert result.published == 2
    assert ctx.paths.splits_path().exists()
    assert ctx.paths.dividends_path().exists()

    splits = storage.read_parquet(ctx.paths.splits_path())
    assert set(splits.column("ticker").to_pylist()) == {"NVDA", "DPU", "AFOVF"}
    assert splits.num_rows == len(SPLIT_ROWS)


def test_corporate_actions_are_cumulative_and_idempotent(ctx):
    jobs.run_corporate_actions(ctx, today=DAY)
    first = storage.read_parquet(ctx.paths.splits_path()).num_rows

    jobs.run_corporate_actions(ctx, today=NEXT_DAY)
    second = storage.read_parquet(ctx.paths.splits_path())
    # Same rows re-fetched: merged on (ticker, execution_date), not duplicated.
    assert second.num_rows == first
    assert rules.check_no_duplicate_keys(second, "splits") is None


def test_a_revised_corporate_action_replaces_the_row_it_corrects(ctx):
    jobs.run_corporate_actions(ctx, today=DAY)
    original = storage.read_parquet(ctx.paths.splits_path())
    before = next(r for r in original.to_pylist() if r["id"] == "Esplit1")
    assert before["split_to"] == 10.0

    # The provider revises the ratio for that same split id.
    revised = [dict(r) for r in SPLIT_ROWS]
    revised[0]["split_to"] = 4
    table = parse.splits(revised, ingested_at=INGESTED_AT).table
    merged = storage.merge_rows(original, table, "splits")

    rows = [r for r in merged.to_pylist() if r["id"] == "Esplit1"]
    assert len(rows) == 1  # corrected, not duplicated
    assert rows[0]["split_to"] == 4.0
    assert merged.num_rows == original.num_rows


def test_a_negative_dividend_is_quarantined(ctx):
    bad = [dict(DIVIDEND_ROWS[0], cash_amount=-1.0)]
    table = storage.normalise(parse.dividends(bad, ingested_at=INGESTED_AT).table, "dividends")
    finding = rules.check_dividend_amounts_sane(table)
    assert finding is not None and finding.is_hard


def test_a_non_positive_split_ratio_is_quarantined(ctx):
    bad = [dict(SPLIT_ROWS[0], split_from=0)]
    table = storage.normalise(parse.splits(bad, ingested_at=INGESTED_AT).table, "splits")
    finding = rules.check_split_ratios_sane(table)
    assert finding is not None and finding.is_hard


def test_corporate_actions_refetch_a_lookback_window_not_just_new_rows(ctx):
    """Revisions to already-recorded actions are only seen by looking back."""
    jobs.run_corporate_actions(ctx, today=DAY)
    requested = [p for p in ctx.api.requested if "splits" in p]
    assert requested
    assert jobs.CORPORATE_ACTION_LOOKBACK_DAYS >= 30


# --------------------------------------------------------------------------- #
# The split cross-check, wired into EOD
# --------------------------------------------------------------------------- #


def test_splits_for_day_reads_the_published_table(ctx):
    jobs.run_corporate_actions(ctx, today=DAY)
    tickers, ratios = jobs.splits_for_day(ctx, DAY)
    assert tickers == {"NVDA"}  # DPU executes in December
    assert ratios["NVDA"] == pytest.approx(10.0)

    assert jobs.splits_for_day(ctx, date(2026, 12, 17))[0] == {"DPU"}
    assert jobs.splits_for_day(ctx, date(2026, 1, 2)) == (set(), {})


def test_splits_for_day_is_empty_before_any_are_collected(ctx):
    assert jobs.splits_for_day(ctx, DAY) == (set(), {})


def test_a_split_explained_move_is_no_longer_flagged(ctx):
    """The M5 payoff: a 10:1 split stops looking like a 90% crash.

    Reviewed against NVDA's 10:1 execution on 2026-09-29, as the plan asks.
    """
    jobs.run_corporate_actions(ctx, today=DAY)
    split_tickers, split_ratios = jobs.splits_for_day(ctx, DAY)

    # A 10:1 split: the close falls to a tenth.
    table = pa.table(
        {
            "ticker": ["NVDA", "OTHER"],
            "date": [DAY, DAY],
            "open": [to_scaled("18.0"), to_scaled("100.0")],
            "high": [to_scaled("19.0"), to_scaled("101.0")],
            "low": [to_scaled("17.0"), to_scaled("20.0")],
            "close": [to_scaled("18.0"), to_scaled("25.0")],
            "vwap": [to_scaled("18.0"), to_scaled("50.0")],
            "volume": [to_scaled("1000.0"), to_scaled("1000.0")],
            "transactions": [10, 10],
            "ingested_at": [INGESTED_AT, INGESTED_AT],
        },
        schema=schemas.EOD_SCHEMA,
    )
    previous = {"NVDA": to_scaled("180.0"), "OTHER": to_scaled("100.0")}

    without = rules.check_large_moves(table, previous_closes=previous)
    assert without is not None
    assert "NVDA" in " ".join(without.samples)

    with_splits = rules.check_large_moves(
        table, previous_closes=previous, split_tickers=split_tickers
    )
    # NVDA explained; OTHER's 75% drop still flagged for review.
    assert with_splits is not None
    assert "NVDA" not in " ".join(with_splits.samples)
    assert "OTHER" in " ".join(with_splits.samples)
    assert split_ratios["NVDA"] == pytest.approx(10.0)


def test_the_eod_job_passes_split_context_through(ctx, monkeypatch):
    seen: dict = {}
    real_run = checks.run

    def spy(table, dataset, batch_id, context):
        if dataset == "eod":
            seen["split_tickers"] = context.split_tickers
        return real_run(table, dataset, batch_id, context)

    monkeypatch.setattr(checks, "run", spy)
    jobs.run_corporate_actions(ctx, today=DAY)

    # Serve an EOD day so the job has something to publish.
    eod_payload = {
        "status": "OK",
        "adjusted": False,
        "results": [
            {"T": "NVDA", "o": 18.0, "h": 19.0, "l": 17.0, "c": 18.0, "v": 1000.0, "t": 0, "n": 5}
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if "grouped" in request.url.path:
            day = request.url.path.rsplit("/", 1)[-1]
            payload = dict(eod_payload)
            payload["results"] = [
                dict(
                    eod_payload["results"][0],
                    t=int(datetime.fromisoformat(f"{day}T20:00:00+00:00").timestamp() * 1000),
                )
            ]
            return httpx.Response(200, json=payload)
        return ctx.api(request)

    ctx.client.http = httpx.Client(
        transport=httpx.MockTransport(handler), base_url="https://api.massive.com"
    )
    # Only 2026-09-29 should be owed, since that is when the split executes.
    ctx.manifest.expect("eod", [date(2026, 9, 28)])
    ctx.manifest.mark_coverage("eod", date(2026, 9, 28), manifest_module.OK)

    jobs.run_eod(ctx, today=NEXT_DAY, backfill_days=1)
    assert seen.get("split_tickers") == {"NVDA"}


# --------------------------------------------------------------------------- #
# Verification and status
# --------------------------------------------------------------------------- #


def test_reference_and_corporate_action_batches_verify(ctx):
    jobs.run_reference(ctx, today=DAY)
    jobs.run_corporate_actions(ctx, today=DAY)
    assert ctx.manifest.verify_checksums(ctx.paths.root) == []


def test_status_rolls_up_both_corporate_action_datasets(ctx):
    from collector import status as status_module

    reference = jobs.run_reference(ctx, today=DAY)
    actions = jobs.run_corporate_actions(ctx, today=DAY)
    jobs.write_status(ctx, [reference, actions])

    written = status_module.read(ctx.paths.status_file)
    assert written is not None
    assert written.jobs["reference"].coverage == {manifest_module.OK: 1}
    # splits and dividends both counted under the one job.
    assert written.jobs["corporate_actions"].coverage == {manifest_module.OK: 2}
    assert written.healthy is True


def test_a_clean_reference_run_passes_every_applicable_check(ctx):
    table = storage.normalise(
        parse.tickers(TICKER_ROWS, snapshot=DAY, ingested_at=INGESTED_AT).table, "tickers"
    )
    report = checks.run(table, "tickers", "b1", CheckContext(day=DAY))
    assert report.hard_failures == [], [f.message for f in report.hard_failures]
    # The bar-only checks are skipped rather than crashing on absent columns.
    assert "timestamps_in_window" not in report.passed
    assert "snapshot_date" in report.passed
