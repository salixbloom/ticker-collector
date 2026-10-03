"""The provider's rolling history window.

Measured on 2026-10-02: both minute and EOD served 2024-10-04 and refused
2024-10-01 with ``403 NOT_AUTHORIZED``. The window is ~730 days and its edge moves
forward every day, so a date not collected before it ages out is gone for good.

The deployment failure this came from: `backfill_days = 730` asked for precisely
the refused date, the refusal was treated as retryable, and the minute job spent
its entire 1,200-call budget per run relearning it -- 502,000 ticker-days owed and
not one minute bar collected.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import httpx
import pytest

from collector import jobs, storage
from collector import manifest as manifest_module
from collector.checks.trading_calendar import TradingCalendar
from collector.cli import main
from collector.config import CollectionConfig, Config, SourceConfig, UniverseConfig
from collector.manifest import Manifest
from collector.ratelimit import RateLimiter
from collector.source import Client, NotAuthorizedError

CAL = TradingCalendar()
TODAY = date(2026, 10, 2)
#: The real boundary, from the live probe.
SERVED_FROM = date(2024, 10, 4)
INGESTED_AT = datetime(2026, 10, 2, 2, 0, tzinfo=UTC)

NOT_AUTHORIZED_BODY = {
    "status": "NOT_AUTHORIZED",
    "request_id": "4ad6ca4d3c6580bcd42da4410d44b149",
    "message": "Your plan doesn't include this data timeframe. "
    "Please upgrade your plan at https://polygon.io/pricing",
}


@pytest.fixture(autouse=True)
def api_key(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-test-history-123456")


class WindowedApi:
    """Serves data only inside a rolling window, as the provider measurably does.

    Two behaviours, both verified against the live API on 2026-10-02:

    * a request wholly before the window is refused with ``403 NOT_AUTHORIZED``;
    * a *range* straddling the edge returns ``200 OK`` and silently clips -- asking
      for 2024-09-27..2024-11-15 came back with its first bar on 2024-10-02.
    """

    def __init__(self, served_from: date = SERVED_FROM, *, bars: int = 3) -> None:
        self.served_from = served_from
        self.bars = bars
        self.calls: list[tuple[str, str]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        parts = path.strip("/").split("/")
        requested = date.fromisoformat(parts[-1])
        start = date.fromisoformat(parts[-2]) if "range" in path else requested
        self.calls.append((path, start.isoformat()))

        if requested < self.served_from:
            # The whole range is outside the window.
            return httpx.Response(403, json=NOT_AUTHORIZED_BODY)

        if "range" in path and start < self.served_from:
            # Straddles the edge: clip to the window rather than refuse.
            start = self.served_from

        if "grouped" in path:
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "adjusted": False,
                    "results": [
                        {"T": "AAA", "o": 1.0, "h": 1.5, "l": 0.5, "c": 1.2, "v": 100.5, "n": 3}
                    ],
                },
            )

        results = []
        for day in CAL.trading_days(start, requested):
            session = CAL.session(day)
            if session is None:
                continue
            for i in range(self.bars):
                results.append(
                    {
                        "t": int((session[0] + timedelta(minutes=i)).timestamp() * 1000),
                        "o": 100.0,
                        "h": 100.5,
                        "l": 99.5,
                        "c": 100.25,
                        "vw": 100.1,
                        "v": 1000.5,
                        "n": 7,
                    }
                )
        return httpx.Response(200, json={"status": "OK", "adjusted": False, "results": results})


def make_ctx(tmp_path, api, *, backfill_days: int = 730, margin: int = 5) -> jobs.Context:
    config = Config(
        data_root=tmp_path / "data",
        source=SourceConfig(
            base_url="https://api.massive.com", calls_per_minute=10_000, max_retries=0
        ),
        collection=CollectionConfig(backfill_days=backfill_days, history_safety_days=margin),
        universe=UniverseConfig(size=2),
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
    api = WindowedApi()
    context = make_ctx(tmp_path, api)
    context.api = api  # type: ignore[attr-defined]
    yield context
    context.client.close()
    context.manifest.close()


# --------------------------------------------------------------------------- #
# The client distinguishes a plan limit from a transient failure
# --------------------------------------------------------------------------- #


def test_a_plan_limit_raises_its_own_error(tmp_path):
    """Not a SourceError, because retrying it can never succeed."""
    ctx = make_ctx(tmp_path, WindowedApi())
    try:
        with pytest.raises(NotAuthorizedError, match="NOT_AUTHORIZED"):
            ctx.client.grouped_daily(date(2024, 10, 1))
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_a_plan_limit_is_not_retried(tmp_path):
    """Retrying would spend the budget on calls that cannot succeed."""
    api = WindowedApi()
    ctx = make_ctx(tmp_path, api)
    try:
        with pytest.raises(NotAuthorizedError):
            ctx.client.grouped_daily(date(2024, 10, 1))
        assert len(api.calls) == 1
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_a_403_without_a_not_authorized_body_stays_a_plain_error(tmp_path):
    """A bad key also gives 403; only the provider's own marker means a plan limit."""
    from collector.source import SourceError

    def forbidden(_request):
        return httpx.Response(403, text="nope")

    ctx = make_ctx(tmp_path, forbidden)
    try:
        with pytest.raises(SourceError) as caught:
            ctx.client.grouped_daily(date(2024, 10, 1))
        assert not isinstance(caught.value, NotAuthorizedError)
    finally:
        ctx.client.close()
        ctx.manifest.close()


# --------------------------------------------------------------------------- #
# The floor: learned, persisted, one-way
# --------------------------------------------------------------------------- #


def test_the_configured_window_stays_inside_the_edge(ctx):
    """`backfill_days = 730` with no margin asked for exactly the refused date."""
    floor = jobs.history_floor(ctx, "eod", today=TODAY)
    assert floor == TODAY - timedelta(days=725)
    # Which is strictly inside a 730-day window.
    assert (TODAY - floor).days < 730


def test_a_refusal_raises_the_floor_and_persists_it(ctx):
    refused = date(2024, 10, 1)
    jobs.note_unauthorized(ctx, "eod", refused, today=TODAY)

    # The probe found the real edge rather than crawling one day forward.
    assert ctx.manifest.history_floor("eod") == SERVED_FROM
    # Survives a reopen, so a restart does not rediscover it.
    ctx.manifest.close()
    with Manifest(ctx.paths.manifest_db) as reopened:
        assert reopened.history_floor("eod") == SERVED_FROM


def test_the_floor_only_ever_moves_forward(ctx):
    """The window rolls forward; a lower floor would re-ask proven-dead dates."""
    ctx.manifest.raise_history_floor("eod", date(2025, 1, 2))
    assert ctx.manifest.raise_history_floor("eod", date(2024, 6, 1)) is False
    assert ctx.manifest.history_floor("eod") == date(2025, 1, 2)
    assert ctx.manifest.raise_history_floor("eod", date(2025, 3, 1)) is True
    assert ctx.manifest.history_floor("eod") == date(2025, 3, 1)


def test_the_floor_is_per_dataset(ctx):
    ctx.manifest.raise_history_floor("eod", date(2025, 1, 2))
    assert ctx.manifest.history_floor("minute") is None


def test_the_learned_floor_overrides_a_wider_configured_window(ctx):
    ctx.manifest.raise_history_floor("minute", date(2025, 6, 1))
    assert jobs.history_floor(ctx, "minute", today=TODAY) == date(2025, 6, 1)


def test_expected_days_never_reach_below_the_floor(ctx):
    ctx.manifest.raise_history_floor("eod", date(2026, 9, 1))
    days = jobs.expected_eod_days(ctx, today=TODAY)
    assert min(days) >= date(2026, 9, 1)

    ctx.manifest.raise_history_floor("minute", date(2026, 9, 15))
    assert min(jobs.expected_minute_days(ctx, today=TODAY)) >= date(2026, 9, 15)


# --------------------------------------------------------------------------- #
# Bulk retirement: one refusal, not hundreds of thousands of calls
# --------------------------------------------------------------------------- #


def test_one_refusal_retires_every_older_outstanding_unit(ctx):
    """The fix for 502,000 owed ticker-days ground through 1,200 calls a run."""
    days = CAL.trading_days(date(2024, 9, 20), date(2024, 10, 10))
    tickers = [f"T{i:03d}" for i in range(50)]
    ctx.manifest.expect("minute", days, tickers=tickers)
    owed_before = len(ctx.manifest.gaps("minute"))
    assert owed_before == len(days) * len(tickers)

    retired = jobs.note_unauthorized(ctx, "minute", date(2024, 10, 1), today=TODAY)

    assert retired > 0
    remaining = ctx.manifest.gaps("minute")
    assert all(g.day >= SERVED_FROM for g in remaining)
    assert len(remaining) < owed_before
    # And they are retired, not failed, so they are never retried.
    summary = ctx.manifest.coverage_summary("minute")
    assert summary[manifest_module.SKIPPED] == retired


def test_retirement_leaves_already_collected_units_alone(ctx):
    days = CAL.trading_days(date(2024, 9, 20), date(2024, 10, 10))
    ctx.manifest.expect("eod", days)
    ctx.manifest.mark_coverage("eod", days[0], manifest_module.OK)

    jobs.note_unauthorized(ctx, "eod", date(2024, 10, 1), today=TODAY)
    states = {
        row["day"]: row["state"]
        for row in ctx.manifest.conn.execute(
            "SELECT day, state FROM coverage WHERE dataset = 'eod'"
        )
    }
    assert states[days[0].isoformat()] == manifest_module.OK


def test_the_retirement_reason_says_the_data_is_gone(ctx):
    ctx.manifest.expect("eod", [date(2024, 9, 20)])
    jobs.note_unauthorized(ctx, "eod", date(2024, 10, 1), today=TODAY)
    gap = next(
        row
        for row in ctx.manifest.conn.execute("SELECT last_error FROM coverage WHERE dataset='eod'")
    )
    assert "no longer be fetched" in gap["last_error"]


# --------------------------------------------------------------------------- #
# The jobs, end to end against a windowed provider
# --------------------------------------------------------------------------- #


def test_eod_collects_what_it_can_and_retires_the_rest(tmp_path):
    """The deployment symptom: it must not stall on the unreachable edge."""
    api = WindowedApi()
    # A window deliberately reaching past the provider's edge, as 730 did.
    ctx = make_ctx(tmp_path, api, backfill_days=740, margin=0)
    try:
        result = jobs.run_eod(ctx, today=TODAY, max_days=40)

        assert result.published > 0, "it must collect the days it is allowed"
        assert result.skipped > 0, "and retire the ones it is not"
        # Nothing below the learned floor is still owed.
        floor = ctx.manifest.history_floor("eod")
        assert floor is not None
        assert all(g.day >= floor for g in ctx.manifest.gaps("eod"))
        # It did not spend a call per refused date.
        refused = [d for _p, d in api.calls if date.fromisoformat(d) < SERVED_FROM]
        assert len(refused) <= 2
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_eod_does_not_treat_the_edge_as_a_run_failure(tmp_path):
    """Aged-out days are expected, not faults; the run must stay healthy."""
    ctx = make_ctx(tmp_path, WindowedApi(), backfill_days=740, margin=0)
    try:
        result = jobs.run_eod(ctx, today=TODAY, max_days=20)
        assert result.outcome in ("ok", "soft_flags"), result.errors
        assert not result.aborted
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_the_second_run_wastes_no_calls_on_the_dead_edge(tmp_path):
    """The floor is persisted, so the next run does not rediscover it."""
    api = WindowedApi()
    ctx = make_ctx(tmp_path, api, backfill_days=740, margin=0)
    try:
        jobs.run_eod(ctx, today=TODAY, max_days=20)
        api.calls.clear()
        jobs.run_eod(ctx, today=TODAY, max_days=20)
        refused = [d for _p, d in api.calls if date.fromisoformat(d) < SERVED_FROM]
        assert refused == []
    finally:
        ctx.client.close()
        ctx.manifest.close()


def seed_minute_universe(ctx: jobs.Context) -> None:
    import pyarrow as pa

    from collector import schemas
    from collector.money import to_scaled

    day = TODAY - timedelta(days=1)
    table = pa.table(
        {
            "ticker": ["AAA", "BBB"],
            "rank": [1, 2],
            "dollar_volume": [to_scaled("100.0"), to_scaled("50.0")],
            "snapshot_date": [day, day],
            "ingested_at": [INGESTED_AT, INGESTED_AT],
        },
        schema=schemas.UNIVERSE_SCHEMA,
    )
    staged = storage.stage(
        table, dataset="universe", paths=ctx.paths, batch_id="u", filename="u.parquet"
    )
    storage.publish(staged, ctx.paths.universe_path(day))


def test_minute_retires_the_dead_edge_instead_of_burning_the_budget(tmp_path):
    """The exact deployment failure: 1,200 calls a run, nothing collected."""
    api = WindowedApi()
    ctx = make_ctx(tmp_path, api, backfill_days=740, margin=0)
    try:
        seed_minute_universe(ctx)
        result = jobs.run_minute(ctx, today=TODAY, max_calls=30)

        refused = [d for _p, d in api.calls if date.fromisoformat(d) < SERVED_FROM]
        # One refusal retires the whole dead range for every ticker.
        assert len(refused) <= 2, f"wasted {len(refused)} calls on refused dates"
        assert result.skipped > 0
        floor = ctx.manifest.history_floor("minute")
        assert floor is not None
        assert all(g.day >= floor for g in ctx.manifest.gaps("minute"))
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_minute_then_makes_progress_on_the_dates_it_can_reach(tmp_path):
    api = WindowedApi()
    ctx = make_ctx(tmp_path, api, backfill_days=740, margin=0)
    try:
        seed_minute_universe(ctx)
        jobs.run_minute(ctx, today=TODAY, max_calls=30)
        served = [d for _p, d in api.calls if date.fromisoformat(d) >= SERVED_FROM]
        assert served, "it must go on to fetch dates inside the window"
        assert any(ctx.paths.minute_parts_dir(date.fromisoformat(d)).exists() for d in served)
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_the_chunked_backfill_also_retires_the_dead_edge(tmp_path):
    api = WindowedApi()
    ctx = make_ctx(tmp_path, api, backfill_days=740, margin=0)
    try:
        seed_minute_universe(ctx)
        result = jobs.run_minute(ctx, today=TODAY, chunk_days=40, max_calls=20)
        # A straddling range is served (clipped), so the only refusals are for
        # ranges wholly outside the window.
        refused = [d for _p, d in api.calls if date.fromisoformat(d) < SERVED_FROM]
        assert len(refused) <= 2
        # And the clipped-away days are retired rather than left owed forever.
        assert result.outcome in ("ok", "soft_flags"), result.errors
        floor = ctx.manifest.history_floor("minute")
        if floor is not None:
            assert all(g.day >= floor for g in ctx.manifest.gaps("minute"))
    finally:
        ctx.client.close()
        ctx.manifest.close()


# --------------------------------------------------------------------------- #
# Configuration and the CLI
# --------------------------------------------------------------------------- #


def test_a_margin_wider_than_the_window_is_rejected(tmp_path):
    from collector.config import ConfigError, load

    path = tmp_path / "c.toml"
    path.write_text(
        "[collection]\nbackfill_days = 10\nhistory_safety_days = 10\n", encoding="utf-8"
    )
    with pytest.raises(ConfigError, match="smaller than backfill_days"):
        load(path)


def test_the_shipped_config_keeps_a_margin():
    """Because 730 with no margin is precisely what the provider refuses."""
    from pathlib import Path

    from collector.config import load

    config = load(Path(__file__).resolve().parents[1] / "config" / "collector.toml")
    assert config.collection.history_safety_days > 0
    assert config.collection.history_safety_days < config.collection.backfill_days


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    config = tmp_path / "collector.toml"
    config.write_text(
        f'[paths]\ndata_root = "{(tmp_path / "data").as_posix()}"\n', encoding="utf-8"
    )
    for name in ("COLLECTOR_DATA_ROOT", "COLLECTOR_CONFIG"):
        monkeypatch.delenv(name, raising=False)
    return config, tmp_path / "data"


def test_cli_history_shows_the_floor(workspace, capsys):
    config, data = workspace
    paths = storage.DataPaths.from_root(data).ensure()
    with Manifest(paths.manifest_db) as man:
        man.raise_history_floor("minute", date(2024, 10, 2))

    assert main(["--config", str(config), "history"]) == 0
    out = capsys.readouterr().out
    assert "configured oldest date" in out
    assert "2024-10-02" in out
    assert "(none yet)" in out  # eod has not been refused


def test_cli_history_reset_forgets_the_floor(workspace, capsys):
    config, data = workspace
    paths = storage.DataPaths.from_root(data).ensure()
    with Manifest(paths.manifest_db) as man:
        man.raise_history_floor("minute", date(2024, 10, 2))

    assert main(["--config", str(config), "history", "--reset"]) == 0
    assert "rediscover" in capsys.readouterr().out
    with Manifest(paths.manifest_db) as man:
        assert man.history_floor("minute") is None


# --------------------------------------------------------------------------- #
# The scheduled minute job must chunk, or a backfill never finishes
# --------------------------------------------------------------------------- #


def test_the_scheduled_minute_job_uses_the_chunked_strategy(tmp_path, monkeypatch):
    """One call per ticker-day would make a two-year backfill take ~400 days.

    1,000 tickers x ~502 trading days is ~502,000 ticker-days. At the default
    1,200 calls a run that is over a year unchunked, against ~11 days chunked.
    """
    from collector.cli import _minute_runner

    api = WindowedApi()
    ctx = make_ctx(tmp_path, api)
    seen: dict = {}

    def spy(_ctx, **kwargs):
        seen.update(kwargs)
        return jobs.JobResult(job="minute", run_id="-")

    monkeypatch.setattr(jobs, "run_minute", spy)
    try:
        _minute_runner(ctx)(ctx)
    finally:
        ctx.client.close()
        ctx.manifest.close()

    assert seen["chunk_days"] == ctx.config.collection.minute_backfill_chunk_days
    assert seen["chunk_days"] > 1
    assert seen["max_calls"] == ctx.config.collection.minute_calls_per_run


def test_chunking_costs_no_extra_calls_in_steady_state(tmp_path):
    """With one day outstanding a chunk collapses to that day, so nothing is lost."""
    api = WindowedApi()
    ctx = make_ctx(tmp_path, api)
    try:
        seed_minute_universe(ctx)
        # Only yesterday is expected, as on a normal evening.
        jobs.run_minute(ctx, today=TODAY, backfill_days=0, chunk_days=40)
        ranges = [p for p, _d in api.calls if "/range/" in p and "/day/" not in p]
        assert ranges, "it should have fetched something"
        for path in ranges:
            parts = path.strip("/").split("/")
            assert parts[-1] == parts[-2], f"expected a single-day request, got {path}"
    finally:
        ctx.client.close()
        ctx.manifest.close()


# --------------------------------------------------------------------------- #
# The edge moving on its own
# --------------------------------------------------------------------------- #


def test_a_day_that_ages_out_stops_being_reported_as_owed(ctx):
    """The leak this closes: no refusal is involved, the window just slid.

    Both bar jobs bound their work by `history_floor`, so a unit left pending
    below it is never worked again -- but it stayed in `gaps()` and so in the
    "still owed" figure. Over a month-long backfill that figure drifts further
    from the truth every day.
    """
    floor = jobs.history_floor(ctx, "eod", today=TODAY)
    aged_out = floor - timedelta(days=30)
    ctx.manifest.expect("eod", [aged_out])
    assert [g.day for g in ctx.manifest.gaps("eod")] == [aged_out]

    retired = jobs.retire_aged_out(ctx, "eod", today=TODAY)

    assert retired == 1
    assert ctx.manifest.gaps("eod") == [], "nothing will ever fill it, so it is not owed"
    summary = ctx.manifest.coverage_summary("eod")
    assert summary.get(manifest_module.SKIPPED) == 1
    row = ctx.manifest.conn.execute(
        "SELECT last_error FROM coverage WHERE dataset = 'eod' AND day = ?",
        (aged_out.isoformat(),),
    ).fetchone()
    assert "history window" in row["last_error"], "and the record says why"


def test_retiring_aged_out_units_leaves_the_reachable_ones_alone(ctx):
    floor = jobs.history_floor(ctx, "eod", today=TODAY)
    reachable = CAL.trading_days(floor, floor + timedelta(days=10))[:3]
    ctx.manifest.expect("eod", [floor - timedelta(days=1), *reachable])

    assert jobs.retire_aged_out(ctx, "eod", today=TODAY) == 1
    assert [g.day for g in ctx.manifest.gaps("eod")] == reachable


def test_an_aged_out_minute_ticker_day_is_retired_without_an_api_call(ctx):
    """One UPDATE for the lot -- a thousand tickers a day cannot each cost a call."""
    floor = jobs.history_floor(ctx, "minute", today=TODAY)
    dead = floor - timedelta(days=7)
    ctx.manifest.expect("minute", [dead], tickers=["AAPL", "MSFT", "NVDA"])
    before = len(ctx.api.calls)  # type: ignore[attr-defined]

    assert jobs.retire_aged_out(ctx, "minute", today=TODAY) == 3
    assert ctx.manifest.gaps("minute") == []
    assert len(ctx.api.calls) == before  # type: ignore[attr-defined]


def test_the_minute_job_sweeps_aged_out_ticker_days_as_it_runs(ctx):
    """So the owed figure in the daily report converges instead of drifting."""
    floor = jobs.history_floor(ctx, "minute", today=TODAY)
    dead = floor - timedelta(days=14)
    ctx.manifest.expect("minute", [dead], tickers=["AAPL"])

    jobs.run_minute(ctx, today=TODAY, max_calls=0, chunk_days=40)

    assert not [g for g in ctx.manifest.gaps("minute") if g.day == dead]
