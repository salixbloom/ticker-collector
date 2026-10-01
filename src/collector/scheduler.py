"""The in-process scheduler.

One long-running process runs every job, because a single process is what lets one
shared rate limiter own the 5-calls-per-minute budget. Two containers, or cron
spawning overlapping processes, could jointly exceed it.

Schedule times only decide *when* catch-up happens: every job is a "fill what is
missing" job, so a job that does not run at 18:30 simply finds the same gap at
07:00. That is why this scheduler can stay this simple -- it has no need to track
missed fires or queue them, since the work is derived from the manifest, not from
the schedule.
"""

from __future__ import annotations

import contextlib
import logging
import signal
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from time import monotonic
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

JobFn = Callable[[], None]


@dataclass
class ScheduledJob:
    """A job and the local times of day it runs at."""

    name: str
    fn: JobFn
    at: tuple[time, ...]
    #: Monthly jobs run only on the first matching day of a month.
    monthly: bool = False
    last_run_date: tuple[int, int, int] | None = field(default=None, init=False)
    last_fired_slot: tuple[int, int, int, int, int] | None = field(default=None, init=False)


class Scheduler:
    """A clock-driven job runner with a stop event.

    Jobs run sequentially in the scheduler thread. That is intentional: it keeps
    one job from racing another for the rate limiter or the manifest, and the
    daily call budget comfortably fits all jobs end to end.
    """

    def __init__(
        self,
        timezone: str = "America/New_York",
        tick_seconds: float = 20.0,
        *,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self.tz = ZoneInfo(timezone)
        self.tick_seconds = tick_seconds
        self.jobs: list[ScheduledJob] = []
        self._stop = threading.Event()
        self._heartbeat: Callable[[], None] | None = None
        self._clock = clock
        #: When to run the startup catch-up pass, if one was requested.
        self._catch_up_at: float | None = None

    # -- registration -------------------------------------------------------- #

    def every_day_at(self, name: str, fn: JobFn, *times: time) -> ScheduledJob:
        job = ScheduledJob(name=name, fn=fn, at=tuple(times))
        self.jobs.append(job)
        return job

    def monthly_at(self, name: str, fn: JobFn, *times: time) -> ScheduledJob:
        job = ScheduledJob(name=name, fn=fn, at=tuple(times), monthly=True)
        self.jobs.append(job)
        return job

    def set_heartbeat(self, fn: Callable[[], None]) -> None:
        """Called every tick, whether or not a job runs."""
        self._heartbeat = fn

    def run_once_on_start(self, *, delay_seconds: float = 120.0) -> None:
        """Run every registered job once, shortly after the loop begins.

        A container deployed at noon would otherwise do nothing until its first
        slot, which looks broken. Every job fills whatever the manifest says is
        missing, so an extra run is idempotent -- the second one finds no gaps.

        The short delay keeps startup responsive: the heartbeat and health check
        come up first, so an operator sees a healthy container immediately rather
        than one that appears to hang while it backfills.
        """
        self._catch_up_at = self._clock() + max(delay_seconds, 0.0)

    # -- lifecycle ----------------------------------------------------------- #

    def install_signal_handlers(self) -> None:
        """Stop cleanly on SIGTERM/SIGINT so ``docker stop`` is not a kill."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            # Not the main thread, or a platform lacking the signal: nothing to do.
            with contextlib.suppress(ValueError, OSError):
                signal.signal(sig, lambda *_: self.stop())

    def stop(self) -> None:
        log.info("stop requested")
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def run_forever(self) -> None:
        log.info(
            "scheduler started, tz=%s, jobs=%s",
            self.tz.key,
            ", ".join(job.name for job in self.jobs) or "(none)",
        )
        while not self._stop.is_set():
            now = datetime.now(self.tz)
            if self._heartbeat is not None:
                self._safely("heartbeat", self._heartbeat, quiet=True)

            if self._catch_up_due():
                self._catch_up_at = None
                log.info("running startup catch-up pass over %d job(s)", len(self.jobs))
                for job in self.jobs:
                    if self._stop.is_set():
                        break
                    self._run(job, datetime.now(self.tz))

            for job in self.jobs:
                if self._stop.is_set():
                    break
                if self._due(job, now):
                    self._run(job, now)
            self._stop.wait(self.tick_seconds)
        log.info("scheduler stopped")

    def _catch_up_due(self) -> bool:
        return self._catch_up_at is not None and self._clock() >= self._catch_up_at

    # -- internals ----------------------------------------------------------- #

    def _due(self, job: ScheduledJob, now: datetime) -> bool:
        """True if one of the job's times has passed within the current window.

        A slot is matched if ``now`` is in ``[slot, slot + grace)`` and that exact
        slot has not fired yet, where the grace window is a few ticks wide. Without
        the grace window a job whose tick lands a second late would skip the day;
        without the fired-slot record it would fire repeatedly inside the window.
        """
        if (
            job.monthly
            and job.last_run_date is not None
            and job.last_run_date[:2]
            == (
                now.year,
                now.month,
            )
        ):
            return False
        grace = timedelta(seconds=max(self.tick_seconds * 3, 60))
        for slot_time in job.at:
            slot = now.replace(
                hour=slot_time.hour,
                minute=slot_time.minute,
                second=slot_time.second,
                microsecond=0,
            )
            if slot <= now < slot + grace:
                key = (now.year, now.month, now.day, slot_time.hour, slot_time.minute)
                if job.last_fired_slot != key:
                    return True
        return False

    def _run(self, job: ScheduledJob, now: datetime) -> None:
        for slot_time in job.at:
            slot = now.replace(
                hour=slot_time.hour, minute=slot_time.minute, second=slot_time.second
            )
            grace = timedelta(seconds=max(self.tick_seconds * 3, 60))
            if slot <= now < slot + grace:
                job.last_fired_slot = (
                    now.year,
                    now.month,
                    now.day,
                    slot_time.hour,
                    slot_time.minute,
                )
                break
        job.last_run_date = (now.year, now.month, now.day)
        log.info("job %s starting", job.name)
        self._safely(job.name, job.fn)

    @staticmethod
    def _safely(name: str, fn: Callable[[], None], *, quiet: bool = False) -> None:
        """Run a callable, logging rather than propagating failures.

        One failing job must not take the scheduler down -- the next run would
        otherwise never happen, and self-healing depends on there being a next run.

        ``quiet`` suppresses the success line only. The heartbeat fires every tick,
        so logging it would add ~1,400 lines a day of nothing; a heartbeat that
        *fails* is still logged, because that is a real symptom.
        """
        try:
            fn()
        except Exception:
            log.exception("job %s failed", name)
        else:
            if not quiet:
                log.info("job %s finished", name)
