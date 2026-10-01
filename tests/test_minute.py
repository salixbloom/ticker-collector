"""M6/M7: the universe, and minute collection that resumes mid-run."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pyarrow as pa
import pytest

from collector import checks, jobs, schemas, storage
from collector import manifest as manifest_module
from collector.checks.trading_calendar import EXCHANGE_TZ, TradingCalendar
from collector.config import CollectionConfig, Config, SourceConfig, UniverseConfig
from collector.manifest import Manifest
from collector.money import from_scaled, to_scaled
from collector.ratelimit import RateLimiter
from collector.source import Client

FIXTURES = Path(__file__).parent / "fixtures"
TODAY = date(2026, 9, 30)  # Wednesday
#: The trading day before TODAY, and so the one a single-day run collects.
PREV = date(2026, 9, 29)
DAY = PREV
#: `backfill_days` is a calendar lookback from yesterday, so 0 means "yesterday
#: only" -- 1 would already reach back to Monday the 28th.
ONE_DAY = 0
INGESTED_AT = datetime(2026, 9, 30, 2, 0, tzinfo=UTC)
CAL = TradingCalendar()


@pytest.fixture(autouse=True)
def api_key(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-test-minute-123456")


# --------------------------------------------------------------------------- #
# A fake aggregates endpoint
# --------------------------------------------------------------------------- #


class MinuteApi:
    """Serves minute bars for any ticker and date range.

    Generates one bar per minute of the regular session for each requested day, so
    a multi-day request returns a multi-day response the way the real endpoint
    does.
    """

    def __init__(
        self,
        *,
        bars_per_day: int = 5,
        fail: set[str] | None = None,
        empty: set[str] | None = None,
        flood: set[str] | None = None,
    ) -> None:
        self.bars_per_day = bars_per_day
        self.fail = fail or set()
        self.empty = empty or set()
        #: Tickers that return the full 50,000-bar cap, as a too-wide range would.
        self.flood = flood or set()
        self.calls: list[tuple[str, str, str]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.strip("/").split("/")
        ticker = parts[3]
        start, end = parts[-2], parts[-1]
        self.calls.append((ticker, start, end))

        if ticker in self.fail:
            return httpx.Response(500)
        if ticker in self.flood:
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "adjusted": False,
                    "results": [{"t": 0, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}] * 50_000,
                },
            )
        if ticker in self.empty:
            return httpx.Response(200, json={"status": "OK", "adjusted": False, "results": []})

        results = []
        for day in CAL.trading_days(date.fromisoformat(start), date.fromisoformat(end)):
            session = CAL.session(day)
            if session is None:
                continue
            open_at = session[0]
            for i in range(self.bars_per_day):
                ts = open_at + timedelta(minutes=i)
                results.append(
                    {
                        "t": int(ts.timestamp() * 1000),
                        "o": 100.0,
                        "h": 100.5,
                        "l": 99.5,
                        "c": 100.25,
                        "vw": 100.1,
                        "v": 1000.363143,
                        "n": 12,
                    }
                )
        return httpx.Response(200, json={"status": "OK", "adjusted": False, "results": results})


def make_ctx(tmp_path, api, *, universe_size: int = 3, grace_days: int = 30) -> jobs.Context:
    config = Config(
        data_root=tmp_path / "data",
        source=SourceConfig(base_url="https://api.massive.com", calls_per_minute=10_000),
        collection=CollectionConfig(backfill_days=10, extended_hours=True),
        universe=UniverseConfig(size=universe_size, grace_days=grace_days),
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
        calendar=CAL,
    )


@pytest.fixture
def ctx(tmp_path):
    api = MinuteApi()
    context = make_ctx(tmp_path, api)
    context.api = api  # type: ignore[attr-defined]
    yield context
    context.client.close()
    context.manifest.close()


# --------------------------------------------------------------------------- #
# EOD groundwork the universe needs
# --------------------------------------------------------------------------- #


def publish_eod(ctx: jobs.Context, days, volumes: dict[str, float]) -> None:
    """Publish EOD days so the universe has something to rank."""
    for day in days:
        rows: dict[str, list] = {name: [] for name in schemas.EOD_SCHEMA.names}
        for ticker, volume in volumes.items():
            rows["ticker"].append(ticker)
            rows["date"].append(day)
            rows["open"].append(to_scaled("100.0"))
            rows["high"].append(to_scaled("101.0"))
            rows["low"].append(to_scaled("99.0"))
            rows["close"].append(to_scaled("100.0"))
            rows["vwap"].append(to_scaled("100.0"))
            rows["volume"].append(to_scaled(str(volume)))
            rows["transactions"].append(10)
            rows["ingested_at"].append(INGESTED_AT)
        table = pa.table(rows, schema=schemas.EOD_SCHEMA)
        destination = ctx.paths.eod_path(day)
        existing = storage.read_parquet(destination) if destination.exists() else None
        merged = storage.merge_rows(existing, table, "eod")
        staged = storage.stage(
            merged,
            dataset="eod",
            paths=ctx.paths,
            batch_id=f"eod-seed-{day.isoformat()}",
            filename="eod.parquet",
        )
        storage.publish(staged, destination)


# --------------------------------------------------------------------------- #
# M6: the universe
# --------------------------------------------------------------------------- #


def test_dollar_volume_does_not_overflow_on_a_megacap(ctx):
    """close x volume carries 1e12 in scaled units; a liquid name reaches ~1.3e22.

    int64 tops out at 9.2e18, so computing the product before dividing the scale
    out would wrap. These are real AAPL-sized numbers.
    """
    publish_eod(ctx, [PREV], {"AAPL": 38_478_037.237258})
    ranked = jobs.dollar_volume_by_ticker(ctx, [PREV])

    # 100.0 x 38,478,037.237258 = 3,847,803,723.7258, exactly.
    assert from_scaled(ranked["AAPL"]) == Decimal("3847803723.7258")
    assert ranked["AAPL"] < 2**63 - 1


def test_the_universe_ranks_by_dollar_volume_and_is_capped(ctx):
    publish_eod(
        ctx,
        CAL.recent_trading_days(PREV, 5),
        {"BIG": 9_000_000.0, "MID": 5_000_000.0, "SMALL": 1_000_000.0, "TINY": 10.0},
    )
    result = jobs.run_universe(ctx, today=TODAY, size=3)
    assert result.published == 1

    table = storage.read_parquet(ctx.paths.universe_path(TODAY))
    assert table.column("ticker").to_pylist() == ["BIG", "MID", "SMALL"]
    assert table.column("rank").to_pylist() == [1, 2, 3]
    # Monotonically decreasing dollar volume.
    volumes = table.column("dollar_volume").to_pylist()
    assert volumes == sorted(volumes, reverse=True)


def test_the_universe_snapshot_is_dated_and_accumulates(ctx):
    """A past universe must stay recoverable, or a backtest cannot be honest."""
    publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"A": 100.0, "B": 50.0})
    jobs.run_universe(ctx, today=TODAY, size=2)
    jobs.run_universe(ctx, today=date(2026, 10, 1), size=1)

    names = sorted(p.name for p in ctx.paths.published.glob("reference/universe_*.parquet"))
    assert names == ["universe_2026-09-30.parquet", "universe_2026-10-01.parquet"]
    assert storage.read_parquet(ctx.paths.universe_path(TODAY)).num_rows == 2
    assert storage.read_parquet(ctx.paths.universe_path(date(2026, 10, 1))).num_rows == 1


def test_the_universe_costs_no_api_calls(ctx):
    """It is derived from data already on disk."""
    publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"A": 100.0})
    before = ctx.client.calls_made
    jobs.run_universe(ctx, today=TODAY)
    assert ctx.client.calls_made == before


def test_the_universe_fails_clearly_with_no_eod_data(ctx):
    result = jobs.run_universe(ctx, today=TODAY)
    assert result.outcome == "hard_failure"
    assert "collect that first" in result.errors[0]
    assert [g.day for g in ctx.manifest.gaps("universe")] == [TODAY]


def test_rebuilding_the_universe_twice_in_a_day_is_a_no_op(ctx):
    publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"A": 100.0})
    jobs.run_universe(ctx, today=TODAY)
    assert jobs.run_universe(ctx, today=TODAY).published == 0


def test_a_non_trading_day_builds_nothing(ctx):
    publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"A": 100.0})
    assert jobs.run_universe(ctx, today=date(2026, 10, 3)).published == 0  # Saturday


# --------------------------------------------------------------------------- #
# M6: universe stability (the grace window)
# --------------------------------------------------------------------------- #


def test_a_ticker_that_leaves_the_universe_is_still_collected(ctx):
    """The plan's reason: a name hovering at the cutoff would otherwise leave
    holes in its minute history that cannot be refilled once the 2-year window
    passes."""
    publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"STAY": 100.0, "LEAVER": 50.0})
    jobs.run_universe(ctx, today=TODAY, size=2)
    assert set(jobs.effective_universe(ctx, TODAY)) == {"STAY", "LEAVER"}

    # A month later LEAVER has dropped out of the ranking.
    later = date(2026, 10, 15)
    publish_eod(ctx, CAL.recent_trading_days(later - timedelta(days=1), 5), {"STAY": 100.0})
    jobs.run_universe(ctx, today=later, size=1)

    current = jobs.latest_universe(ctx, later)
    assert current is not None and current[1] == ["STAY"]
    # Still collected, because it was in the universe inside the grace window.
    assert set(jobs.effective_universe(ctx, later)) == {"STAY", "LEAVER"}


def test_a_long_departed_ticker_is_dropped_once_the_grace_window_passes(tmp_path):
    ctx = make_ctx(tmp_path, MinuteApi(), grace_days=10)
    try:
        publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"STAY": 100.0, "LEAVER": 50.0})
        jobs.run_universe(ctx, today=TODAY, size=2)

        far = date(2026, 12, 1)  # well past the 10-day grace
        publish_eod(ctx, CAL.recent_trading_days(date(2026, 11, 30), 5), {"STAY": 100.0})
        jobs.run_universe(ctx, today=far, size=1)
        assert jobs.effective_universe(ctx, far) == ["STAY"]
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_the_effective_universe_is_empty_before_any_snapshot(ctx):
    assert jobs.effective_universe(ctx, TODAY) == []


# --------------------------------------------------------------------------- #
# M7: the daily minute job
# --------------------------------------------------------------------------- #


def seed_universe(ctx: jobs.Context, tickers: dict[str, float], *, size: int | None = None) -> None:
    publish_eod(ctx, CAL.recent_trading_days(PREV, 5), tickers)
    jobs.run_universe(ctx, today=TODAY, size=size or len(tickers))


def test_a_minute_day_publishes_one_file_for_every_ticker(ctx):
    seed_universe(ctx, {"AAA": 300.0, "BBB": 200.0, "CCC": 100.0})
    result = jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)

    assert result.published == 1, result.errors + result.quarantine_reasons
    path = ctx.paths.minute_path(PREV)
    assert path.exists()
    table = storage.read_parquet(path)
    assert sorted(set(table.column("ticker").to_pylist())) == ["AAA", "BBB", "CCC"]
    assert table.num_rows == 3 * 5  # 3 tickers x 5 bars
    assert ctx.manifest.gaps("minute") == []


def test_one_call_per_ticker_day_on_the_daily_path(ctx):
    seed_universe(ctx, {"AAA": 300.0, "BBB": 200.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)
    assert len(ctx.api.calls) == 2
    # Each call asks for a single day.
    assert all(start == end for _t, start, end in ctx.api.calls)


def test_the_published_day_passes_its_checks(ctx):
    seed_universe(ctx, {"AAA": 300.0, "BBB": 200.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)

    table = storage.read_parquet(ctx.paths.minute_path(PREV))
    report = checks.run(table, "minute", "check", checks.CheckContext(day=PREV, calendar=CAL))
    assert report.hard_failures == [], [f.message for f in report.hard_failures]


def test_running_the_minute_job_twice_does_nothing_the_second_time(ctx):
    seed_universe(ctx, {"AAA": 300.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)
    calls = len(ctx.api.calls)

    second = jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)
    assert second.published == 0
    assert len(ctx.api.calls) == calls


def test_a_ticker_with_no_bars_is_skipped_not_retried_forever(ctx):
    """An illiquid name genuinely does not trade some days."""
    api = MinuteApi(empty={"QUIET"})
    ctx.client.http = httpx.Client(
        transport=httpx.MockTransport(api), base_url="https://api.massive.com"
    )
    ctx.api = api
    seed_universe(ctx, {"AAA": 300.0, "QUIET": 200.0})

    result = jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)
    assert result.published == 1
    assert ctx.manifest.gaps("minute") == []  # not owed
    summary = ctx.manifest.coverage_summary("minute")
    assert summary[manifest_module.SKIPPED] == 1
    assert summary[manifest_module.OK] == 1
    # And the day still published, with the absence explained rather than flagged.
    assert (
        "QUIET"
        not in storage.read_parquet(ctx.paths.minute_path(PREV)).column("ticker").to_pylist()
    )


def test_a_failing_ticker_leaves_the_day_unpublished_and_owed(tmp_path):
    """A day is published only when nothing is outstanding for it."""
    api = MinuteApi(fail={"BROKEN"})
    ctx = make_ctx(tmp_path, api)
    try:
        seed_universe(ctx, {"AAA": 300.0, "BROKEN": 200.0})
        result = jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)

        assert result.published == 0
        assert result.errors
        assert not ctx.paths.minute_path(PREV).exists()
        assert [g.ticker for g in ctx.manifest.gaps("minute")] == ["BROKEN"]
        # The part that did succeed is kept, so the retry only fetches BROKEN.
        assert ctx.paths.minute_part(PREV, "AAA").exists()
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_the_day_publishes_once_the_failing_ticker_recovers(tmp_path):
    api = MinuteApi(fail={"BROKEN"})
    ctx = make_ctx(tmp_path, api)
    try:
        seed_universe(ctx, {"AAA": 300.0, "BROKEN": 200.0})
        jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)
        calls_during_outage = len(api.calls)

        api.fail.clear()
        result = jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)

        assert result.published == 1
        # Only the one outstanding ticker was re-fetched.
        assert len(api.calls) == calls_during_outage + 1
        assert sorted(
            set(storage.read_parquet(ctx.paths.minute_path(PREV)).column("ticker").to_pylist())
        ) == ["AAA", "BROKEN"]
    finally:
        ctx.client.close()
        ctx.manifest.close()


# --------------------------------------------------------------------------- #
# M6: resuming mid-run -- the definition of done
# --------------------------------------------------------------------------- #


def test_the_minute_backfill_resumes_after_being_stopped_mid_run(ctx):
    """M6's definition of done, exercised by stopping on a call budget."""
    seed_universe(ctx, {"AAA": 500.0, "BBB": 400.0, "CCC": 300.0})

    first = jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY, max_calls=2)
    assert first.published == 0  # the day is incomplete
    assert len(ctx.manifest.gaps("minute")) == 1
    # Two tickers are already on disk as parts.
    parts = sorted(p.stem for p in ctx.paths.minute_parts_dir(PREV).glob("*.parquet"))
    assert len(parts) == 2

    second = jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)
    assert second.published == 1
    assert ctx.manifest.gaps("minute") == []
    table = storage.read_parquet(ctx.paths.minute_path(PREV))
    assert sorted(set(table.column("ticker").to_pylist())) == ["AAA", "BBB", "CCC"]
    # Each ticker fetched exactly once across both runs.
    assert len(ctx.api.calls) == 3


def test_work_parts_survive_a_staging_clear(ctx):
    """`staging/` is wiped at startup; resumable progress must not live there."""
    seed_universe(ctx, {"AAA": 500.0, "BBB": 400.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY, max_calls=1)
    assert any(ctx.paths.minute_parts_dir(PREV).glob("*.parquet"))

    storage.clear_staging(ctx.paths)  # what `collector init` does on every start
    assert any(ctx.paths.minute_parts_dir(PREV).glob("*.parquet"))


def test_parts_are_removed_once_the_day_is_published(ctx):
    seed_universe(ctx, {"AAA": 300.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)
    assert not ctx.paths.minute_parts_dir(PREV).exists()


def test_a_ticker_with_an_awkward_symbol_round_trips(ctx):
    """Real symbols contain dots and mixed case: BRK.B, ASGIrw."""
    seed_universe(ctx, {"BRK.B": 300.0, "ASGIrw": 200.0})
    result = jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)
    assert result.published == 1
    table = storage.read_parquet(ctx.paths.minute_path(PREV))
    assert sorted(set(table.column("ticker").to_pylist())) == ["ASGIrw", "BRK.B"]


# --------------------------------------------------------------------------- #
# M6: the backfill strategy
# --------------------------------------------------------------------------- #


def test_chunked_backfill_uses_far_fewer_calls(ctx):
    """One call per ticker per range, not per ticker-day."""
    seed_universe(ctx, {"AAA": 300.0, "BBB": 200.0})
    result = jobs.run_minute(ctx, today=TODAY, backfill_days=10, chunk_days=40)

    days = jobs.expected_minute_days(ctx, today=TODAY, backfill_days=10)
    assert len(days) > 1
    # 2 tickers x 1 call each, versus 2 x len(days) on the daily path.
    assert len(ctx.api.calls) == 2
    assert result.published == len(days)

    for day in days:
        table = storage.read_parquet(ctx.paths.minute_path(day))
        assert sorted(set(table.column("ticker").to_pylist())) == ["AAA", "BBB"]
        assert set(table.column("ts").to_pylist()) != set()


def test_a_multi_day_response_is_split_across_daily_partitions(ctx):
    seed_universe(ctx, {"AAA": 300.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=10, chunk_days=40)

    days = jobs.expected_minute_days(ctx, today=TODAY, backfill_days=10)
    for day in days:
        table = storage.read_parquet(ctx.paths.minute_path(day))
        # Every bar in a day's file belongs to that exchange day.
        local_days = {ts.astimezone(EXCHANGE_TZ).date() for ts in table.column("ts").to_pylist()}
        assert local_days == {day}


def test_hitting_the_bar_cap_narrows_the_chunk_rather_than_storing_a_truncation(tmp_path):
    """At 50,000 bars a complete response is indistinguishable from a cut-off one."""
    api = MinuteApi(flood={"DENSE"})
    ctx = make_ctx(tmp_path, api)
    try:
        seed_universe(ctx, {"DENSE": 300.0})
        jobs.run_minute(ctx, today=TODAY, backfill_days=10, chunk_days=40)

        widths = [
            (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
            for _t, start, end in api.calls
        ]
        # It halved repeatedly rather than accepting the capped response.
        assert len(widths) > 1
        assert widths[-1] < widths[0]
        assert not any(ctx.paths.published.glob("minute/**/*.parquet"))
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_the_call_budget_bounds_a_backfill_run(ctx):
    seed_universe(ctx, {"AAA": 500.0, "BBB": 400.0, "CCC": 300.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=10, chunk_days=40, max_calls=2)
    assert len(ctx.api.calls) == 2
    assert ctx.manifest.gaps("minute")  # the rest is still owed


# --------------------------------------------------------------------------- #
# Guard rails
# --------------------------------------------------------------------------- #


def test_the_minute_job_refuses_to_run_without_a_universe(ctx):
    result = jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)
    assert result.published == 0
    assert "universe" in result.errors[0]
    assert ctx.api.calls == []


def test_expected_minute_days_end_yesterday(ctx):
    days = jobs.expected_minute_days(ctx, today=TODAY, backfill_days=10)
    assert TODAY not in days
    assert max(days) == PREV
    assert all(CAL.is_trading_day(d) for d in days)


def test_a_zero_window_means_yesterday_only(ctx):
    """And is not silently replaced by the configured 730-day window."""
    assert jobs.expected_minute_days(ctx, today=TODAY, backfill_days=0) == [PREV]
    assert jobs.expected_eod_days(ctx, today=TODAY, backfill_days=0) == [PREV]


def test_reconciliation_context_is_supplied_when_eod_exists(ctx):
    """So the minute<->EOD checks actually run on a published day."""
    seed_universe(ctx, {"AAA": 300.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)

    row = ctx.manifest.latest_published("minute", PREV)
    assert row is not None
    report = json.loads(row["check_json"])
    assert "minute_eod_reconciliation" in report["checks_passed"] or any(
        "reconciliation" in f["check"] for f in report["soft"]
    )


def test_targeting_tickers_narrows_the_work_not_what_the_day_owes(ctx):
    """Otherwise a targeted re-fetch would publish a day missing universe members."""
    seed_universe(ctx, {"AAA": 300.0, "BBB": 200.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY, tickers=["AAA"])

    # BBB is still owed, so the day stays unpublished.
    assert [g.ticker for g in ctx.manifest.gaps("minute")] == ["BBB"]
    assert not ctx.paths.minute_path(PREV).exists()
    assert [t for t, _s, _e in ctx.api.calls] == ["AAA"]


def test_the_gate_writes_no_phantom_day_level_coverage_row_for_minute(ctx):
    """Minute coverage is per ticker; a `ticker=''` row would be unclearable.

    The gate marks day-level coverage failed when a batch is quarantined. For
    minute that row could never be cleared by any ticker fetch, so the day would
    never publish again.
    """
    seed_universe(ctx, {"AAA": 300.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)

    rows = list(ctx.manifest.conn.execute("SELECT ticker FROM coverage WHERE dataset = 'minute'"))
    assert [r["ticker"] for r in rows] == ["AAA"]
    assert ctx.manifest.coverage_summary("minute") == {manifest_module.OK: 1}


def test_minute_batches_verify(ctx):
    seed_universe(ctx, {"AAA": 300.0, "BBB": 200.0})
    jobs.run_minute(ctx, today=TODAY, backfill_days=ONE_DAY)
    assert ctx.manifest.verify_checksums(ctx.paths.root) == []


# --------------------------------------------------------------------------- #
# M7: compression, measured on real bars
# --------------------------------------------------------------------------- #

REAL_MINUTE = FIXTURES / "minute_aapl_2026-09-29.json"


@pytest.mark.skipif(not REAL_MINUTE.exists(), reason="recorded minute fixture not present")
def test_zstd_level_choice_is_justified_by_measurement(paths):
    """M7 asks for the level to be chosen and logged, not assumed.

    Measured on real recorded bars for four tickers. Asserts the ordering the
    choice rests on: 9 is meaningfully smaller than 3, and 15 adds little.
    """
    from collector import parse

    tables = []
    for name, ticker in (
        ("minute_aapl_2026-09-29.json", "AAPL"),
        ("minute_MSFT.json", "MSFT"),
        ("minute_F.json", "F"),
        ("minute_SOFI.json", "SOFI"),
    ):
        path = FIXTURES / name
        if not path.exists():
            continue
        rows = json.loads(path.read_text())["results"]
        tables.append(
            parse.minute(rows, ticker=ticker, day=DAY, calendar=CAL, ingested_at=INGESTED_AT).table
        )
    table = storage.normalise(pa.concat_tables(tables), "minute")

    sizes = {}
    for level in (3, 9, 15):
        staged = storage.stage(
            table,
            dataset="minute",
            paths=paths,
            batch_id=f"z{level}",
            filename="m.parquet",
            compression_level=level,
        )
        sizes[level] = staged.bytes
        assert storage.read_parquet(staged.path).num_rows == table.num_rows

    # 9 beats 3; 15 buys very little more. Both are why the default is 9.
    assert sizes[9] < sizes[3]
    assert sizes[15] <= sizes[9]
    assert (sizes[9] - sizes[15]) / sizes[9] < 0.10, f"level 15 saved more than expected: {sizes}"
    print(
        f"\nminute bytes/row by zstd level: "
        f"{ {lvl: round(b / table.num_rows, 1) for lvl, b in sizes.items()} }"
    )


# --------------------------------------------------------------------------- #
# Universe instrument types
# --------------------------------------------------------------------------- #


def publish_tickers(ctx: jobs.Context, snapshot: date, types: dict[str, str]) -> None:
    """Publish a reference snapshot so the type filter has something to read."""
    from collector import parse

    rows = [
        {"ticker": ticker, "type": kind, "active": True, "name": ticker}
        for ticker, kind in types.items()
    ]
    # One inactive name, so the snapshot sanity check is satisfied.
    rows.append({"ticker": "DEAD", "type": "CS", "active": False, "name": "Dead"})
    table = parse.tickers(rows, snapshot=snapshot, ingested_at=INGESTED_AT).table
    staged = storage.stage(
        table,
        dataset="tickers",
        paths=ctx.paths,
        batch_id=f"tickers-{snapshot.isoformat()}",
        filename="t.parquet",
    )
    storage.publish(staged, ctx.paths.tickers_path(snapshot))


def test_without_a_type_filter_etfs_rank_alongside_stocks(ctx):
    """Measured on real data: SPY and QQQ outrank every actual stock."""
    publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"SPY": 900.0, "AAA": 100.0})
    jobs.run_universe(ctx, today=TODAY, size=2)
    assert storage.read_parquet(ctx.paths.universe_path(TODAY)).column("ticker").to_pylist() == [
        "SPY",
        "AAA",
    ]


def test_a_type_filter_excludes_etfs(tmp_path):
    ctx = make_ctx(tmp_path, MinuteApi())
    object.__setattr__(ctx.config.universe, "types", ("CS",))
    try:
        publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"SPY": 900.0, "AAA": 100.0})
        publish_tickers(ctx, PREV, {"SPY": "ETF", "AAA": "CS"})

        jobs.run_universe(ctx, today=TODAY, size=5)
        assert storage.read_parquet(ctx.paths.universe_path(TODAY)).column(
            "ticker"
        ).to_pylist() == ["AAA"]
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_a_type_filter_with_no_reference_snapshot_fails_rather_than_passing_everything(tmp_path):
    """Silently ignoring the filter would admit the instruments it excludes."""
    ctx = make_ctx(tmp_path, MinuteApi())
    object.__setattr__(ctx.config.universe, "types", ("CS",))
    try:
        publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"SPY": 900.0})
        result = jobs.run_universe(ctx, today=TODAY, size=5)
        assert result.outcome == "hard_failure"
        assert "reference snapshot" in " ".join(result.errors)
        assert not ctx.paths.universe_path(TODAY).exists()
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_a_type_filter_matching_nothing_is_a_hard_failure(tmp_path):
    ctx = make_ctx(tmp_path, MinuteApi())
    object.__setattr__(ctx.config.universe, "types", ("WARRANT",))
    try:
        publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"AAA": 100.0})
        publish_tickers(ctx, PREV, {"AAA": "CS"})
        result = jobs.run_universe(ctx, today=TODAY, size=5)
        assert result.outcome == "hard_failure"
        assert "matched no ticker" in " ".join(result.errors)
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_the_type_filter_is_recorded_in_the_manifest_params(tmp_path):
    ctx = make_ctx(tmp_path, MinuteApi())
    object.__setattr__(ctx.config.universe, "types", ("CS",))
    try:
        publish_eod(ctx, CAL.recent_trading_days(PREV, 5), {"AAA": 100.0})
        publish_tickers(ctx, PREV, {"AAA": "CS"})
        jobs.run_universe(ctx, today=TODAY, size=5)

        row = ctx.manifest.latest_published("universe", TODAY)
        assert row is not None
        assert '"types": ["CS"]' in row["params_json"]
    finally:
        ctx.client.close()
        ctx.manifest.close()
