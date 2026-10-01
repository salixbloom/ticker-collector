"""The data provider client.

Covers the endpoints the plan needs, all read-only GETs:

* grouped daily bars -- one call returns every ticker's bar for one date
* per-ticker aggregate bars -- used for minute data, up to 50,000 bars per call
* splits, dividends -- corporate actions
* tickers -- reference data, including inactive (delisted) tickers

**``adjusted=false`` is sent on every aggregate request, and the response is
checked to confirm it.** The endpoint defaults to ``adjusted=true``, verified
against the live API: a request with no ``adjusted`` parameter comes back with
``"adjusted": true`` in the envelope. Storing that would put split-adjusted
prices on disk, and those change retroactively every time a split happens --
destroying the point-in-time guarantee the whole design rests on. The default is
the dangerous one here, so it is both sent explicitly and verified on the way
back rather than trusted.

**Partial responses never reach the caller.** A paginated fetch either returns
every page or raises; there is no path that yields the pages it managed to get.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

import httpx

from .config import Config
from .ratelimit import RateLimiter

log = logging.getLogger(__name__)

#: Max bars the aggregates endpoint returns per call (confirmed in the docs).
MAX_AGGREGATE_LIMIT = 50_000

#: Max rows per page on the reference endpoints.
MAX_REFERENCE_LIMIT = 1_000

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class SourceError(RuntimeError):
    """A request failed in a way the caller must handle."""


class PartialResponse(SourceError):
    """Pagination could not be completed, so the result would be incomplete."""


class NotAdjustedError(SourceError):
    """The provider returned adjusted data when raw was requested."""


@dataclass
class Response:
    """One decoded API response."""

    status: int
    payload: dict[str, Any]
    request_id: str | None = None

    @property
    def results(self) -> list[dict[str, Any]]:
        results = self.payload.get("results")
        if results is None:
            return []
        if isinstance(results, dict):
            return [results]
        return list(results)

    @property
    def next_url(self) -> str | None:
        return self.payload.get("next_url")


@dataclass
class Client:
    """Rate-limited, retrying client for one API key."""

    config: Config
    limiter: RateLimiter
    http: httpx.Client | None = None
    sleep: Callable[[float], None] = time.sleep
    #: Injectable for tests; real jitter avoids synchronised retries.
    jitter: Callable[[], float] = random.random
    calls_made: int = field(default=0, init=False)
    retries_made: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if self.http is None:
            self.http = httpx.Client(
                base_url=self.config.source.base_url,
                timeout=self.config.source.timeout_seconds,
                follow_redirects=True,
            )

    def close(self) -> None:
        if self.http is not None:
            self.http.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- transport ----------------------------------------------------------- #

    def _headers(self) -> dict[str, str]:
        # The key goes in a header, never a query parameter: query strings end up
        # in logs, proxies and error messages far more readily than headers do.
        return {
            "Authorization": f"Bearer {self.config.api_key}",
            "Accept": "application/json",
        }

    def get(self, path_or_url: str, params: dict[str, Any] | None = None) -> Response:
        """One GET, rate-limited, with retries on 429 and 5xx.

        Retries use exponential backoff with jitter and honour ``Retry-After``
        when the provider sends it.
        """
        assert self.http is not None
        attempts = self.config.source.max_retries + 1
        last_error: Exception | None = None

        for attempt in range(attempts):
            self.limiter.acquire()
            self.calls_made += 1
            try:
                response = self.http.get(path_or_url, params=params, headers=self._headers())
            except httpx.HTTPError as exc:
                last_error = SourceError(f"{type(exc).__name__} requesting {path_or_url}")
                log.warning("request failed (%s), attempt %d/%d", exc, attempt + 1, attempts)
            else:
                if response.status_code not in RETRYABLE_STATUS:
                    if response.status_code >= 400:
                        raise SourceError(
                            f"HTTP {response.status_code} for {path_or_url}: {response.text[:200]}"
                        )
                    return Response(
                        status=response.status_code,
                        payload=response.json(),
                        request_id=response.headers.get("X-Request-Id"),
                    )

                last_error = SourceError(f"HTTP {response.status_code} for {path_or_url}")
                retry_after = _retry_after_seconds(response)
                if response.status_code == 429:
                    # Our window and the provider's disagree; defer every caller.
                    self.limiter.penalise(retry_after or self.limiter.window)
                log.warning("HTTP %d, attempt %d/%d", response.status_code, attempt + 1, attempts)

            if attempt < attempts - 1:
                self.retries_made += 1
                self.sleep(self._backoff(attempt))

        raise last_error or SourceError(f"exhausted retries for {path_or_url}")

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with full jitter, capped."""
        return min(2.0**attempt, 60.0) * (0.5 + 0.5 * self.jitter())

    def paginate(
        self, path: str, params: dict[str, Any] | None = None, *, max_pages: int = 1_000
    ) -> list[dict[str, Any]]:
        """Follow ``next_url`` to the end, returning every result.

        Returns a list rather than a generator deliberately: a generator lets a
        caller consume a few pages and treat them as the whole answer, and a
        half-fetched corporate-actions table is worse than no table.
        """
        results: list[dict[str, Any]] = []
        response = self.get(path, params)
        results.extend(response.results)

        pages = 1
        while response.next_url:
            if pages >= max_pages:
                raise PartialResponse(
                    f"{path}: stopped after {pages} pages with more remaining; "
                    "refusing to return a partial result"
                )
            response = self.get(response.next_url)
            results.extend(response.results)
            pages += 1
        return results

    # -- endpoints ----------------------------------------------------------- #

    def grouped_daily(self, day: date, *, include_otc: bool = False) -> Response:
        """Every ticker's daily bar for one date. One call per trading day.

        Survivorship-free by construction: the response includes tickers that
        later delist, because it is a snapshot of what traded that day.
        """
        response = self.get(
            f"/v2/aggs/grouped/locale/us/market/stocks/{day.isoformat()}",
            {"adjusted": "false", "include_otc": str(include_otc).lower()},
        )
        _assert_unadjusted(response, f"grouped_daily {day.isoformat()}")
        return response

    def aggregates(
        self,
        ticker: str,
        start: date,
        end: date,
        *,
        timespan: str = "minute",
        multiplier: int = 1,
        limit: int = MAX_AGGREGATE_LIMIT,
    ) -> Response:
        """Aggregate bars for one ticker over a date range."""
        response = self.get(
            f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/"
            f"{start.isoformat()}/{end.isoformat()}",
            {"adjusted": "false", "sort": "asc", "limit": limit},
        )
        _assert_unadjusted(response, f"aggregates {ticker} {start}..{end}")
        if len(response.results) >= limit:
            # At the cap we cannot tell a complete answer from a truncated one.
            raise PartialResponse(
                f"aggregates {ticker} {start}..{end} returned {len(response.results)} bars, "
                f"at the {limit} limit; narrow the date range"
            )
        return response

    def splits(self, *, executed_after: date | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": MAX_REFERENCE_LIMIT,
            "order": "asc",
            "sort": "execution_date",
        }
        if executed_after is not None:
            params["execution_date.gte"] = executed_after.isoformat()
        return self.paginate("/v3/reference/splits", params)

    def dividends(self, *, ex_after: date | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": MAX_REFERENCE_LIMIT,
            "order": "asc",
            "sort": "ex_dividend_date",
        }
        if ex_after is not None:
            params["ex_dividend_date.gte"] = ex_after.isoformat()
        return self.paginate("/v3/reference/dividends", params)

    def tickers(
        self, *, active: bool | None = None, market: str = "stocks"
    ) -> list[dict[str, Any]]:
        """Ticker reference data.

        ``active=None`` means *both* active and inactive, fetched as two separate
        paginated passes and concatenated.

        **Omitting the parameter does not do that.** Verified against the live
        API: a request with no ``active`` parameter returns only active tickers
        (13,248 of them, 0 inactive), so relying on the default would silently
        produce a survivorship-biased snapshot -- the one thing this dataset
        exists to prevent. ``check_ticker_snapshot_sanity`` caught exactly this
        and stays in place as the backstop.
        """
        if active is None:
            return self._tickers_page(active=True, market=market) + self._tickers_page(
                active=False, market=market
            )
        return self._tickers_page(active=active, market=market)

    def _tickers_page(self, *, active: bool, market: str) -> list[dict[str, Any]]:
        return self.paginate(
            "/v3/reference/tickers",
            {
                "market": market,
                "active": "true" if active else "false",
                "limit": MAX_REFERENCE_LIMIT,
                "order": "asc",
                "sort": "ticker",
            },
        )


def _assert_unadjusted(response: Response, what: str) -> None:
    """Refuse adjusted data. See the module docstring for why this matters."""
    adjusted = response.payload.get("adjusted")
    if adjusted is True:
        raise NotAdjustedError(
            f"{what}: provider returned adjusted prices despite adjusted=false. "
            "Raw prices are required; adjustment happens at read time."
        )


def _retry_after_seconds(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        when = datetime.strptime(raw, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=UTC)
    except ValueError:
        return None
    return max((when - datetime.now(UTC)).total_seconds(), 0.0)


def build(config: Config, limiter: RateLimiter | None = None) -> Client:
    """A client with the configured rate limit."""
    return Client(
        config=config,
        limiter=limiter or RateLimiter(config.source.calls_per_minute),
    )


def iter_date_chunks(start: date, end: date, days: int) -> Iterator[tuple[date, date]]:
    """Split a range into chunks, for keeping aggregate calls under the bar cap."""
    from datetime import timedelta

    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)
