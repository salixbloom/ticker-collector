"""M3: the shared limiter never exceeds N calls in any 60-second window."""

from __future__ import annotations

import threading

import pytest

from collector.ratelimit import RateLimiter


class FakeClock:
    """A controllable monotonic clock whose sleep advances time."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def limiter(clock: FakeClock, calls: int = 5) -> RateLimiter:
    return RateLimiter(calls, window=60.0, clock=clock.time, sleep=clock.sleep)


def test_the_first_n_calls_do_not_wait(clock):
    rl = limiter(clock)
    assert [rl.acquire() for _ in range(5)] == [0.0] * 5
    assert clock.sleeps == []


def test_the_n_plus_first_call_waits_for_the_window(clock):
    rl = limiter(clock)
    for _ in range(5):
        rl.acquire()
    waited = rl.acquire()
    assert waited == pytest.approx(60.0)
    assert clock.now == pytest.approx(1060.0)


def test_calls_spread_over_the_window_never_wait(clock):
    rl = limiter(clock)
    total = 0.0
    for _ in range(50):
        total += rl.acquire()
        clock.advance(12.1)  # 5 calls per 60.5s, just inside the budget
    assert total == 0.0


def test_never_exceeds_the_budget_over_a_simulated_half_hour(clock):
    """M3's definition of done, over 30 simulated minutes of hammering."""
    rl = limiter(clock)
    acquisitions: list[float] = []

    while clock.now < 1000.0 + 1800.0:
        rl.acquire()
        acquisitions.append(clock.now)
        clock.advance(0.01)  # a caller that would otherwise go flat out

    # No 60-second window anywhere in the run contains more than 5 calls.
    worst = 0
    for i, start in enumerate(acquisitions):
        in_window = sum(1 for t in acquisitions[i:] if t < start + 60.0)
        worst = max(worst, in_window)
    assert worst <= 5, f"{worst} calls in some 60s window"
    # And it did real work rather than deadlocking.
    assert len(acquisitions) >= 150


def test_a_penalty_defers_every_caller(clock):
    """After a 429 the provider's accounting wins over ours."""
    rl = limiter(clock)
    rl.penalise(30.0)
    assert rl.in_window == 5
    waited = rl.acquire()
    assert waited == pytest.approx(30.0)


def test_a_non_positive_penalty_is_ignored(clock):
    rl = limiter(clock)
    rl.penalise(0)
    assert rl.in_window == 0


def test_in_window_decays_as_time_passes(clock):
    rl = limiter(clock)
    for _ in range(5):
        rl.acquire()
    assert rl.in_window == 5
    clock.advance(61.0)
    assert rl.in_window == 0


def test_every_attempt_counts_even_a_failed_one(clock):
    """Probed live: five 400s were followed by a 429, so failures consume quota.

    The limiter records on acquire, before the request happens, so a caller whose
    request raises has still spent its slot.
    """
    rl = limiter(clock)
    for _ in range(5):
        rl.acquire()
        try:
            raise RuntimeError("HTTP 400")
        except RuntimeError:
            pass
    assert rl.in_window == 5
    assert rl.acquire() == pytest.approx(60.0)


def test_total_counters_are_tracked(clock):
    rl = limiter(clock)
    for _ in range(6):
        rl.acquire()
    assert rl.total_acquired == 6
    assert rl.total_waited == pytest.approx(60.0)


def test_rejects_a_nonsense_budget():
    with pytest.raises(ValueError, match="at least 1"):
        RateLimiter(0)


def test_is_thread_safe_under_contention():
    """Real threads and a real (tiny) window, since the point is the locking."""
    rl = RateLimiter(5, window=0.5)
    acquired: list[float] = []
    lock = threading.Lock()

    def worker():
        for _ in range(4):
            rl.acquire()
            with lock:
                acquired.append(rl._clock())

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert len(acquired) == 20
    acquired.sort()
    for i, start in enumerate(acquired):
        assert sum(1 for t in acquired[i:] if t < start + 0.5) <= 5
