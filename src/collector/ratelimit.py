"""The shared rate limiter.

One process owns the API key, and one limiter owns the call budget, because the
provider's limit is per key rather than per job. Two jobs with their own limiters
would each stay under 5 calls a minute and jointly exceed it.

A sliding window, not a token bucket or a fixed fence: the provider's limit is
"no more than N calls in any 60-second window", and a fixed window would allow
2N calls across a boundary (five at 11:59:59 and five at 12:00:01).

**Every attempt counts, including failures.** Probed against the live API: five
requests that returned ``400`` were followed by ``429`` on the sixth, so rejected
requests consume quota. A limiter that only counted successes would be
permanently over budget during an outage, exactly when backoff matters most.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable

log = logging.getLogger(__name__)

WINDOW_SECONDS = 60.0


class RateLimiter:
    """Allows at most ``calls`` acquisitions in any ``window`` seconds.

    Thread-safe, and the clock and sleep function are injectable so the tests can
    verify the window over simulated hours without waiting.
    """

    def __init__(
        self,
        calls: int,
        window: float = WINDOW_SECONDS,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if calls < 1:
            raise ValueError("calls must be at least 1")
        self.calls = calls
        self.window = window
        self._clock = clock
        self._sleep = sleep
        self._history: deque[float] = deque()
        self._lock = threading.Lock()
        self.total_acquired = 0
        self.total_waited = 0.0

    def _prune(self, now: float) -> None:
        cutoff = now - self.window
        while self._history and self._history[0] <= cutoff:
            self._history.popleft()

    def acquire(self) -> float:
        """Block until a call is permitted, then record it. Returns time waited.

        Recording happens here rather than after the HTTP call so that a request
        which raises still counts against the budget.
        """
        waited = 0.0
        while True:
            with self._lock:
                now = self._clock()
                self._prune(now)
                if len(self._history) < self.calls:
                    self._history.append(now)
                    self.total_acquired += 1
                    self.total_waited += waited
                    return waited
                # Wait until the oldest call leaves the window.
                delay = self._history[0] + self.window - now
            delay = max(delay, 0.0)
            if delay > 0:
                log.debug("rate limiter: waiting %.2fs", delay)
                self._sleep(delay)
                waited += delay
            else:
                # Clock did not advance; yield rather than spin.
                self._sleep(0.001)
                waited += 0.001

    def penalise(self, seconds: float) -> None:
        """Treat the next ``seconds`` as fully consumed, after a 429.

        The provider has told us we are over budget, so its accounting differs
        from ours. Filling the window defers every caller instead of letting them
        retry straight into another rejection.
        """
        if seconds <= 0:
            return
        with self._lock:
            now = self._clock()
            self._prune(now)
            # Push the window forward so the next slot opens `seconds` from now.
            target = now + seconds - self.window
            self._history.clear()
            self._history.extend([target] * self.calls)
        log.warning("rate limiter: penalised for %.1fs after a 429", seconds)

    @property
    def in_window(self) -> int:
        """Calls currently counted in the window."""
        with self._lock:
            self._prune(self._clock())
            return len(self._history)

    def __repr__(self) -> str:
        return f"RateLimiter(calls={self.calls}, window={self.window}, used={self.in_window})"
