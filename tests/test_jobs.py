"""M4: the EOD job -- self-healing, idempotent, and bounded."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from collector import jobs, storage
from collector import manifest as manifest_module
from collector.checks.trading_calendar import TradingCalendar
from collector.config import ChecksConfig, CollectionConfig, Config, SourceConfig
from collector.manifest import Manifest
from collector.ratelimit import RateLimiter
from collector.source import Client

FIXTURES = Path(__file__).parent / "fixtures"
TODAY = date(2026, 9, 30)

pytestmark = pytest.mark.skipif(
    not (FIXTURES / "grouped_2026-09-29.json").exists(),
    reason="recorded API fixtures not present",
)


@pytest.fixture(scope="module")
def recorded_results() -> list[dict]:
    """A real grouped-daily payload, trimmed to keep these tests quick."""
    results = json.loads((FIXTURES / "grouped_2026-09-29.json").read_text())["results"]
    return results[:400]


@pytest.fixture(autouse=True)
def api_key(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-test-jobs-123456")


class FakeApi:
    """Serves the recorded payload for any date, and counts calls per date."""

    def __init__(self, results: list[dict], *, fail_dates: set[str] | None = None) -> None:
        self.results = results
        self.fail_dates = fail_dates or set()
        self.requested: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        day = request.url.path.rsplit("/", 1)[-1]
        self.requested.append(day)
        if day in self.fail_dates:
            return httpx.Response(500)
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "adjusted": False,
                "resultsCount": len(self.results),
                "results": self.results,
            },
        )


def make_context(tmp_path, api: FakeApi, *, abort_ratio: float = 0.20) -> jobs.Context:
    config = Config(
        data_root=tmp_path / "data",
        source=SourceConfig(base_url="https://api.massive.com", calls_per_minute=10_000),
        collection=CollectionConfig(backfill_days=20),
        checks=ChecksConfig(run_failure_abort_ratio=abort_ratio),
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
def ctx(tmp_path, recorded_results):
    api = FakeApi(recorded_results)
    context = make_context(tmp_path, api)
    context.api = api  # type: ignore[attr-defined]
    yield context
    context.client.close()
    context.manifest.close()


# --------------------------------------------------------------------------- #
# Expected days
# --------------------------------------------------------------------------- #


def test_expected_days_are_trading_days_ending_yesterday(ctx):
    days = jobs.expected_eod_days(ctx, today=TODAY, backfill_days=10)
    assert TODAY not in days  # today's bar does not exist yet
    assert max(days) == date(2026, 9, 29)
    assert all(ctx.calendar.is_trading_day(d) for d in days)
    # No weekends.
    assert not any(d.weekday() >= 5 for d in days)


def test_a_holiday_is_not_expected(ctx):
    days = jobs.expected_eod_days(ctx, today=date(2026, 11, 30), backfill_days=10)
    assert date(2026, 11, 26) not in days  # Thanksgiving
    assert date(2026, 11, 27) in days  # early close, but a trading day


# --------------------------------------------------------------------------- #
# Filling gaps
# --------------------------------------------------------------------------- #


def test_the_job_fills_every_missing_day(ctx):
    result = jobs.run_eod(ctx, today=TODAY, backfill_days=10)
    expected = jobs.expected_eod_days(ctx, today=TODAY, backfill_days=10)

    assert result.published == len(expected)
    assert result.outcome in ("ok", "soft_flags")
    assert ctx.manifest.gaps("eod") == []
    assert result.rows == len(expected) * 400


def test_running_twice_does_nothing_the_second_time(ctx):
    jobs.run_eod(ctx, today=TODAY, backfill_days=10)
    calls_after_first = len(ctx.api.requested)

    second = jobs.run_eod(ctx, today=TODAY, backfill_days=10)
    assert second.published == 0
    assert second.outcome == "ok"
    assert len(ctx.api.requested) == calls_after_first  # no further API calls


def test_max_days_bounds_the_run_and_the_rest_resumes(ctx):
    first = jobs.run_eod(ctx, today=TODAY, backfill_days=10, max_days=3)
    assert first.published == 3
    remaining = len(ctx.manifest.gaps("eod"))
    assert remaining > 0

    second = jobs.run_eod(ctx, today=TODAY, backfill_days=10, max_days=3)
    assert second.published == 3
    assert len(ctx.manifest.gaps("eod")) == remaining - 3


def test_a_failed_day_is_retried_on_the_next_run(tmp_path, recorded_results):
    """The self-healing property: a provider outage repairs itself."""
    failing = date(2026, 9, 24)
    api = FakeApi(recorded_results, fail_dates={failing.isoformat()})
    ctx = make_context(tmp_path, api, abort_ratio=0.9)
    try:
        first = jobs.run_eod(ctx, today=TODAY, backfill_days=10)
        assert first.errors
        assert [g.day for g in ctx.manifest.gaps("eod")] == [failing]

        # The outage ends.
        api.fail_dates.clear()
        second = jobs.run_eod(ctx, today=TODAY, backfill_days=10)
        assert second.published == 1
        assert ctx.manifest.gaps("eod") == []
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_a_mostly_failing_run_aborts_instead_of_hammering(tmp_path, recorded_results):
    every_day = {
        d.isoformat() for d in TradingCalendar().trading_days(date(2026, 9, 1), date(2026, 9, 29))
    }
    api = FakeApi(recorded_results, fail_dates=every_day)
    ctx = make_context(tmp_path, api, abort_ratio=0.20)
    try:
        result = jobs.run_eod(ctx, today=TODAY, backfill_days=20)
        assert result.aborted
        assert result.outcome == "aborted"
        # It stopped early rather than trying all ~20 days.
        assert len(ctx.api.requested) if hasattr(ctx, "api") else True
        assert result.attempted <= 7
    finally:
        ctx.client.close()
        ctx.manifest.close()


# --------------------------------------------------------------------------- #
# Monthly partitioning and idempotency
# --------------------------------------------------------------------------- #


def test_days_accumulate_into_one_monthly_file(ctx):
    jobs.run_eod(ctx, today=TODAY, backfill_days=10)
    files = list(ctx.paths.iter_published("eod"))
    assert len(files) == 1  # one September file
    assert files[0].name == "eod_2026-09.parquet"

    table = storage.read_parquet(files[0])
    expected = jobs.expected_eod_days(ctx, today=TODAY, backfill_days=10)
    assert sorted(set(table.column("date").to_pylist())) == expected
    assert table.num_rows == len(expected) * 400


def test_deleting_a_day_and_re_running_restores_identical_data(ctx):
    """M4's definition of done, as the content checksum.

    The file's bytes legitimately differ on a re-run because ``ingested_at`` is a
    true record of when the download happened. The *data* must be identical, and
    that is what ``content_sha256`` captures -- see storage.content_sha256.
    """
    jobs.run_eod(ctx, today=TODAY, backfill_days=10)
    published = ctx.paths.eod_path(date(2026, 9, 29))
    before = storage.read_parquet(published)
    original_content = storage.content_sha256(before, "eod")
    original_file = storage.sha256_file(published)

    # Drop the file and the coverage, as if the day had been lost.
    published.unlink()
    with ctx.manifest.transaction() as conn:
        conn.execute("DELETE FROM coverage WHERE dataset = 'eod'")
        conn.execute("DELETE FROM batches WHERE dataset = 'eod'")

    jobs.run_eod(ctx, today=TODAY, backfill_days=10)
    after = storage.read_parquet(published)

    assert storage.content_sha256(after, "eod") == original_content
    assert after.drop_columns(["ingested_at"]).equals(
        before.drop_columns(["ingested_at"]), check_metadata=False
    )
    # And the only thing that changed is when we fetched it.
    assert storage.sha256_file(published) != original_file
    assert after.column("ingested_at")[0].as_py() > before.column("ingested_at")[0].as_py()


def test_the_content_checksum_is_recorded_so_restatements_are_detectable(ctx):
    jobs.run_eod(ctx, today=TODAY, backfill_days=10, max_days=2)
    rows = list(
        ctx.manifest.conn.execute(
            "SELECT content_sha256, checksum_sha256 FROM batches WHERE dataset='eod'"
        )
    )
    assert all(r["content_sha256"] and r["checksum_sha256"] for r in rows)
    assert all(r["content_sha256"] != r["checksum_sha256"] for r in rows)


def test_the_manifest_records_both_row_counts(ctx):
    """``rows`` is the monthly file; ``source_rows`` is the day the provider sent.

    Conflating them made a live run reject a good day: the trailing median was a
    month-to-date total, so a single day looked 49.9% short.
    """
    jobs.run_eod(ctx, today=TODAY, backfill_days=10, max_days=3)
    rows = list(
        ctx.manifest.conn.execute(
            "SELECT partition_day, rows, source_rows FROM batches"
            " WHERE dataset='eod' ORDER BY partition_day"
        )
    )
    assert all(r["source_rows"] == 400 for r in rows)
    # The monthly file grows with each day added.
    assert [r["rows"] for r in rows] == [400, 800, 1200]


def test_the_trailing_median_compares_days_not_months(ctx):
    jobs.run_eod(ctx, today=TODAY, backfill_days=10, max_days=4)
    median = jobs._trailing_ticker_median(ctx, date(2026, 9, 29))
    # Each day contributed 400 rows, so the median of days is 400 -- not the
    # 1,600 the monthly `rows` column would have given.
    assert median == 400


def test_no_day_is_quarantined_on_real_recorded_data(ctx):
    result = jobs.run_eod(ctx, today=TODAY, backfill_days=10)
    assert result.quarantined == 0
    assert result.quarantine_reasons == []


# --------------------------------------------------------------------------- #
# adjusted=false, end to end
# --------------------------------------------------------------------------- #


def test_every_request_the_job_makes_asks_for_raw_prices(tmp_path, recorded_results):
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.url.params))
        return httpx.Response(
            200, json={"status": "OK", "adjusted": False, "results": recorded_results[:10]}
        )

    ctx = make_context(tmp_path, handler)  # type: ignore[arg-type]
    try:
        jobs.run_eod(ctx, today=TODAY, backfill_days=5)
    finally:
        ctx.client.close()
        ctx.manifest.close()

    assert seen
    assert all(params["adjusted"] == "false" for params in seen)


# --------------------------------------------------------------------------- #
# Status
# --------------------------------------------------------------------------- #


def test_status_reflects_the_run(ctx):
    from collector import status as status_module

    result = jobs.run_eod(ctx, today=TODAY, backfill_days=10)
    jobs.write_status(ctx, [result])

    written = status_module.read(ctx.paths.status_file)
    assert written is not None
    assert written.jobs["eod"].batches_published == result.published
    assert written.jobs["eod"].coverage.get(manifest_module.OK) == result.published
    assert written.healthy is True


# --------------------------------------------------------------------------- #
# Verification across a merged partition
# --------------------------------------------------------------------------- #


def test_verify_passes_across_a_whole_merged_month(ctx):
    """Several EOD batches share one monthly file, and all must verify.

    A live run caught two bugs here: earlier batches' *file* checksums stop
    matching once a later day rewrites the month (correct history, not
    corruption), and the content checksum must describe the batch's own rows
    rather than the merged month.
    """
    jobs.run_eod(ctx, today=TODAY, backfill_days=10)
    assert len(list(ctx.paths.iter_published("eod"))) == 1
    assert ctx.manifest.verify_checksums(ctx.paths.root) == []


def test_verify_detects_a_restatement_of_one_day_inside_the_month(ctx):
    """The point of a per-day content checksum: one day changing is visible."""
    import pyarrow as pa
    import pyarrow.compute as pc

    jobs.run_eod(ctx, today=TODAY, backfill_days=10)
    assert ctx.manifest.verify_checksums(ctx.paths.root) == []

    # Rewrite one day's closes, as a provider restatement would.
    published = ctx.paths.eod_path(date(2026, 9, 29))
    table = storage.read_parquet(published)
    target = date(2026, 9, 23)
    closes = [
        c + 1 if d == target else c
        for c, d in zip(
            table.column("close").to_pylist(), table.column("date").to_pylist(), strict=True
        )
    ]
    altered = table.set_column(
        table.schema.get_field_index("close"), "close", pa.array(closes, type=pa.int64())
    )
    storage.write_parquet(altered, published, dataset="eod")

    problems = ctx.manifest.verify_checksums(ctx.paths.root)
    offending = [batch_id for batch_id, _ in problems]
    assert any(target.isoformat() in b for b in offending), problems
    # Untouched days still verify.
    assert not any("2026-09-15" in b for b in offending)
    assert pc.sum(altered.column("close")).as_py() != pc.sum(table.column("close")).as_py()


def test_verify_reports_a_missing_published_file(ctx):
    jobs.run_eod(ctx, today=TODAY, backfill_days=10, max_days=2)
    ctx.paths.eod_path(date(2026, 9, 29)).unlink()
    problems = ctx.manifest.verify_checksums(ctx.paths.root)
    assert problems
    assert all("missing file" in why for _, why in problems)
