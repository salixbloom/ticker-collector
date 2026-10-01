"""M3: the API client -- retries, pagination, and the adjusted=false guarantee."""

from __future__ import annotations

from datetime import date

import httpx
import pytest

from collector.config import Config, SourceConfig
from collector.ratelimit import RateLimiter
from collector.source import (
    MAX_AGGREGATE_LIMIT,
    Client,
    NotAdjustedError,
    PartialResponse,
    SourceError,
    iter_date_chunks,
)

DAY = date(2026, 9, 29)


@pytest.fixture(autouse=True)
def api_key(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-test-abcdef123456")


def make_client(
    handler, *, max_retries: int = 2, calls_per_minute: int = 1000, limiter=None
) -> Client:
    """A client wired to a scripted transport, with no real sleeping anywhere.

    The limiter gets a no-op sleep too: a 429 penalty would otherwise make the
    retry wait out the real delay and turn this into a slow test.
    """
    config = Config(
        data_root="/tmp",
        source=SourceConfig(
            base_url="https://api.massive.com",
            calls_per_minute=calls_per_minute,
            max_retries=max_retries,
        ),
    )
    return Client(
        config=config,
        limiter=limiter or RateLimiter(calls_per_minute, sleep=lambda _s: None),
        http=httpx.Client(transport=httpx.MockTransport(handler), base_url=config.source.base_url),
        sleep=lambda _s: None,
        jitter=lambda: 0.5,
    )


def ok(payload: dict) -> httpx.Response:
    return httpx.Response(200, json=payload)


# --------------------------------------------------------------------------- #
# adjusted=false -- the single most consequential detail
# --------------------------------------------------------------------------- #


def test_grouped_daily_sends_adjusted_false():
    """The endpoint defaults to adjusted=true, verified against the live API."""
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.url.params))
        return ok({"status": "OK", "adjusted": False, "results": []})

    make_client(handler).grouped_daily(DAY)
    assert seen["adjusted"] == "false"
    assert seen["include_otc"] == "false"


def test_aggregates_sends_adjusted_false():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.url.params))
        return ok({"status": "OK", "adjusted": False, "results": []})

    make_client(handler).aggregates("AAPL", DAY, DAY)
    assert seen["adjusted"] == "false"
    assert seen["sort"] == "asc"


def test_adjusted_data_is_refused_even_if_we_asked_for_raw():
    """Storing adjusted prices would destroy the point-in-time guarantee."""

    def handler(_request):
        return ok({"status": "OK", "adjusted": True, "results": [{"T": "AAPL"}]})

    with pytest.raises(NotAdjustedError, match="adjusted"):
        make_client(handler).grouped_daily(DAY)

    with pytest.raises(NotAdjustedError):
        make_client(handler).aggregates("AAPL", DAY, DAY)


def test_the_key_goes_in_a_header_not_the_query_string():
    """Query strings leak into logs and proxies far more readily than headers."""
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers.get("Authorization")
        captured["url"] = str(request.url)
        return ok({"status": "OK", "adjusted": False, "results": []})

    make_client(handler).grouped_daily(DAY)
    assert captured["auth"] == "Bearer sk-test-abcdef123456"
    assert "sk-test" not in captured["url"]
    assert "apiKey" not in captured["url"]


# --------------------------------------------------------------------------- #
# Retries
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retryable_statuses_are_retried_then_succeed(status):
    calls = {"n": 0}

    def handler(_request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(status)
        return ok({"status": "OK", "adjusted": False, "results": [{"T": "AAPL"}]})

    client = make_client(handler)
    assert len(client.grouped_daily(DAY).results) == 1
    assert calls["n"] == 2
    assert client.retries_made == 1


def test_retries_are_exhausted_and_then_raise():
    def handler(_request):
        return httpx.Response(503)

    client = make_client(handler, max_retries=2)
    with pytest.raises(SourceError, match="503"):
        client.grouped_daily(DAY)
    assert client.calls_made == 3  # the attempt plus two retries


def test_a_429_penalises_the_shared_limiter_and_the_retry_waits_it_out():
    """Our window and the provider's disagree, so defer every caller.

    The observable effect is that the retry had to wait the Retry-After out
    before it was allowed to go, which is what keeps us from retrying straight
    into another rejection.
    """
    clock = {"now": 0.0}
    waits: list[float] = []

    def fake_sleep(seconds):
        waits.append(seconds)
        clock["now"] += seconds

    limiter = RateLimiter(1000, clock=lambda: clock["now"], sleep=fake_sleep)
    calls = {"n": 0}

    def handler(_request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return ok({"status": "OK", "adjusted": False, "results": []})

    client = make_client(handler, limiter=limiter)
    client.grouped_daily(DAY)

    assert calls["n"] == 2
    assert limiter.total_waited == pytest.approx(7.0)
    assert waits and max(waits) == pytest.approx(7.0)


def test_a_429_without_retry_after_penalises_for_a_full_window():
    clock = {"now": 0.0}

    def fake_sleep(seconds):
        clock["now"] += seconds

    limiter = RateLimiter(1000, clock=lambda: clock["now"], sleep=fake_sleep)
    calls = {"n": 0}

    def handler(_request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429)
        return ok({"status": "OK", "adjusted": False, "results": []})

    make_client(handler, limiter=limiter).grouped_daily(DAY)
    assert limiter.total_waited == pytest.approx(60.0)


def test_a_non_retryable_client_error_fails_immediately():
    calls = {"n": 0}

    def handler(_request):
        calls["n"] += 1
        return httpx.Response(404, text="not found")

    client = make_client(handler)
    with pytest.raises(SourceError, match="404"):
        client.grouped_daily(DAY)
    assert calls["n"] == 1  # no retries on a 404


def test_transport_errors_are_retried():
    calls = {"n": 0}

    def handler(_request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("connection refused")
        return ok({"status": "OK", "adjusted": False, "results": []})

    client = make_client(handler)
    client.grouped_daily(DAY)
    assert calls["n"] == 2


def test_backoff_grows_and_is_capped():
    client = make_client(lambda _r: ok({"status": "OK"}))
    delays = [client._backoff(i) for i in range(10)]
    assert delays[0] < delays[1] < delays[2]
    assert all(d <= 60.0 for d in delays)


def test_every_attempt_consumes_rate_limit_budget():
    def handler(_request):
        return httpx.Response(503)

    client = make_client(handler, max_retries=3, calls_per_minute=100)
    with pytest.raises(SourceError):
        client.grouped_daily(DAY)
    assert client.limiter.total_acquired == 4


# --------------------------------------------------------------------------- #
# Pagination -- never partial
# --------------------------------------------------------------------------- #


def test_pagination_follows_next_url_to_the_end():
    pages = [
        {"status": "OK", "results": [{"id": 1}], "next_url": "https://api.massive.com/p2"},
        {"status": "OK", "results": [{"id": 2}], "next_url": "https://api.massive.com/p3"},
        {"status": "OK", "results": [{"id": 3}]},
    ]
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return ok(pages[len(seen) - 1])

    results = make_client(handler).paginate("/v3/reference/splits")
    assert [r["id"] for r in results] == [1, 2, 3]
    assert seen == ["/v3/reference/splits", "/p2", "/p3"]


def test_pagination_that_will_not_terminate_raises_rather_than_truncating():
    """A half-fetched corporate-actions table is worse than no table."""

    def handler(_request):
        return ok({"status": "OK", "results": [{"id": 1}], "next_url": "https://api.massive.com/n"})

    with pytest.raises(PartialResponse, match="partial"):
        make_client(handler).paginate("/v3/reference/splits", max_pages=5)


def test_a_failure_mid_pagination_propagates():
    calls = {"n": 0}

    def handler(_request):
        calls["n"] += 1
        if calls["n"] == 1:
            return ok(
                {"status": "OK", "results": [{"id": 1}], "next_url": "https://api.massive.com/p2"}
            )
        return httpx.Response(404)

    with pytest.raises(SourceError):
        make_client(handler).paginate("/v3/reference/splits")


def test_aggregates_at_the_bar_cap_is_treated_as_truncated():
    """At the limit a complete answer is indistinguishable from a cut-off one."""

    def handler(_request):
        return ok(
            {
                "status": "OK",
                "adjusted": False,
                "results": [{"t": i} for i in range(MAX_AGGREGATE_LIMIT)],
            }
        )

    with pytest.raises(PartialResponse, match="narrow the date range"):
        make_client(handler).aggregates("AAPL", DAY, DAY)


def test_results_handles_a_single_object_and_a_missing_key():
    def handler(_request):
        return ok({"status": "OK", "results": {"ticker": "AAPL"}})

    assert make_client(handler).get("/x").results == [{"ticker": "AAPL"}]

    def empty(_request):
        return ok({"status": "OK"})

    assert make_client(empty).get("/x").results == []


# --------------------------------------------------------------------------- #
# Reference endpoints
# --------------------------------------------------------------------------- #


def test_tickers_makes_two_passes_to_get_active_and_inactive():
    """Omitting `active` returns only active names, verified against the live API.

    Delisted tickers are what keep the dataset survivorship-free, and the only way
    to get them is to ask for them explicitly.
    """
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.url.params))
        return ok({"status": "OK", "results": [{"ticker": "X"}]})

    rows = make_client(handler).tickers()
    assert [params["active"] for params in seen] == ["true", "false"]
    assert all(params["market"] == "stocks" for params in seen)
    assert len(rows) == 2  # both passes concatenated


def test_tickers_can_be_filtered_when_asked():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.url.params))
        return ok({"status": "OK", "results": []})

    make_client(handler).tickers(active=False)
    assert seen["active"] == "false"


def test_splits_and_dividends_can_be_fetched_incrementally():
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.url.params))
        return ok({"status": "OK", "results": []})

    client = make_client(handler)
    client.splits(executed_after=DAY)
    client.dividends(ex_after=DAY)
    assert seen[0]["execution_date.gte"] == DAY.isoformat()
    assert seen[1]["ex_dividend_date.gte"] == DAY.isoformat()


# --------------------------------------------------------------------------- #
# Chunking
# --------------------------------------------------------------------------- #


def test_date_chunks_cover_the_range_without_gaps_or_overlap():
    chunks = list(iter_date_chunks(date(2026, 1, 1), date(2026, 1, 10), 3))
    assert chunks == [
        (date(2026, 1, 1), date(2026, 1, 3)),
        (date(2026, 1, 4), date(2026, 1, 6)),
        (date(2026, 1, 7), date(2026, 1, 9)),
        (date(2026, 1, 10), date(2026, 1, 10)),
    ]


def test_a_single_day_is_one_chunk():
    assert list(iter_date_chunks(DAY, DAY, 30)) == [(DAY, DAY)]
