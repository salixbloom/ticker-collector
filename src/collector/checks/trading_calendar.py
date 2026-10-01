"""Exchange calendar: which days are trading days, and when sessions run.

Wraps ``pandas_market_calendars`` so the rest of the collector never touches
pandas. The completeness checks need three things from it: whether a date is a
trading day at all, the regular session's start and end in UTC, and the extended
window, since the collector stores pre- and after-hours bars tagged by session.

Session times are looked up per day rather than assumed, because they move: DST
shifts the UTC session by an hour (13:30-20:00 in September, 14:30-21:00 in
November), and early closes shorten it (2026-11-27 closes at 18:00 UTC, giving
210 regular bars rather than 390). A flat "390 bars" rule would misjudge both.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

from ..schemas import Session

EXCHANGE_TZ = ZoneInfo("America/New_York")

#: Extended-hours window in exchange-local time. The provider returns pre-market
#: from 04:00 and after-hours to 20:00 ET, which is what was observed on a live
#: minute response (08:00-23:59 UTC on 2026-09-29 = 04:00-19:59 ET).
PRE_MARKET_OPEN = time(4, 0)
AFTER_HOURS_CLOSE = time(20, 0)

#: A regular full session: 09:30-16:00 ET.
REGULAR_FULL_SESSION_BARS = 390


class TradingCalendar:
    """Trading days and session boundaries for one exchange."""

    def __init__(self, name: str = "NYSE") -> None:
        self.name = name
        self._calendar = None

    @property
    def calendar(self):
        # Imported lazily: pandas_market_calendars pulls in pandas, and the
        # storage and manifest layers have no need for it.
        if self._calendar is None:
            import pandas_market_calendars as mcal

            self._calendar = mcal.get_calendar(self.name)
        return self._calendar

    # -- trading days -------------------------------------------------------- #

    def trading_days(self, start: date, end: date) -> list[date]:
        """Every trading day in ``[start, end]`` inclusive."""
        if end < start:
            return []
        days = self.calendar.valid_days(start_date=start.isoformat(), end_date=end.isoformat())
        return [d.date() for d in days]

    def is_trading_day(self, day: date) -> bool:
        return day in set(self.trading_days(day, day))

    def previous_trading_day(self, day: date) -> date | None:
        days = self.trading_days(day - timedelta(days=10), day - timedelta(days=1))
        return days[-1] if days else None

    def recent_trading_days(self, end: date, count: int) -> list[date]:
        """The ``count`` trading days ending at ``end`` (inclusive if it is one)."""
        if count < 1:
            return []
        # Over-fetch by weekends and holidays, then take the tail.
        start = end - timedelta(days=count * 2 + 15)
        return self.trading_days(start, end)[-count:]

    # -- sessions ------------------------------------------------------------ #

    def session(self, day: date) -> tuple[datetime, datetime] | None:
        """Regular session open and close in UTC, or ``None`` on a non-trading day."""
        return _session_cached(self.name, day)

    def regular_bar_count(self, day: date) -> int:
        """Expected minute bars in the regular session, honouring early closes."""
        window = self.session(day)
        if window is None:
            return 0
        open_at, close_at = window
        return int((close_at - open_at).total_seconds() // 60)

    def is_early_close(self, day: date) -> bool:
        return 0 < self.regular_bar_count(day) < REGULAR_FULL_SESSION_BARS

    def extended_window(self, day: date) -> tuple[datetime, datetime] | None:
        """Full collection window in UTC: pre-market open to after-hours close."""
        if self.session(day) is None:
            return None
        start = datetime.combine(day, PRE_MARKET_OPEN, tzinfo=EXCHANGE_TZ)
        end = datetime.combine(day, AFTER_HOURS_CLOSE, tzinfo=EXCHANGE_TZ)
        return start.astimezone(UTC), end.astimezone(UTC)

    def classify(self, ts: datetime, day: date) -> Session | None:
        """Which session a bar-start timestamp falls in, or ``None`` if outside.

        A bar is labelled by its start, so the bar at exactly ``market_close`` is
        the first after-hours bar, and the regular session is ``[open, close)``.
        """
        window = self.session(day)
        extended = self.extended_window(day)
        if window is None or extended is None:
            return None
        open_at, close_at = window
        ext_start, ext_end = extended
        if not ext_start <= ts < ext_end:
            return None
        if ts < open_at:
            return Session.PRE_MARKET
        if ts < close_at:
            return Session.REGULAR
        return Session.AFTER_HOURS


@lru_cache(maxsize=4096)
def _session_cached(calendar_name: str, day: date) -> tuple[datetime, datetime] | None:
    """Per-day session lookup, cached: a minute backfill asks for the same days
    thousands of times, once per ticker."""
    import pandas_market_calendars as mcal

    schedule = mcal.get_calendar(calendar_name).schedule(
        start_date=day.isoformat(), end_date=day.isoformat()
    )
    if schedule.empty:
        return None
    row = schedule.iloc[0]
    return (
        row["market_open"].to_pydatetime().astimezone(UTC),
        row["market_close"].to_pydatetime().astimezone(UTC),
    )


#: Shared default instance. US equities, so NYSE's calendar.
DEFAULT = TradingCalendar("NYSE")
