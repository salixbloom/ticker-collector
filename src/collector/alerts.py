"""Alerting on hard failures and health changes.

A collector that fails silently is worse than one that does not run: the data
quietly stops arriving and nobody notices until a backtest comes out wrong. So a
hard failure posts to a webhook, and the recovery posts too -- knowing something
is fixed matters as much as knowing it broke.

**Alerts are deduplicated and they persist across restarts.** The same condition
fires once, then not again until it has been quiet for a cooldown; otherwise a
provider outage at 5-minute health-check intervals would send hundreds of
messages overnight and train the operator to ignore them. State lives in
``state/alerts.json`` so a container restart does not re-announce everything.

The payload carries ``text`` and ``content`` with the same message because that
covers the two common webhook shapes -- Slack and most self-hosted receivers read
``text``, Discord reads ``content`` -- so one URL works without configuration.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from . import __version__
from .logging_setup import redact

log = logging.getLogger(__name__)

#: Re-send a still-active alert only after this long.
DEFAULT_COOLDOWN = timedelta(hours=6)

#: Give up quickly: an alert that blocks the scheduler is worse than a lost alert.
TIMEOUT_SECONDS = 10.0


def utcnow() -> datetime:
    from datetime import UTC

    return datetime.now(UTC)


@dataclass
class Alerter:
    """Posts deduplicated alerts to a webhook.

    With no webhook configured every method still works and simply logs, so
    callers never need to check whether alerting is set up.
    """

    webhook: str | None = None
    state_path: Path | None = None
    cooldown: timedelta = DEFAULT_COOLDOWN
    hostname: str = field(default_factory=lambda: os.environ.get("HOSTNAME", "collector"))
    #: Injectable for tests.
    post: Any = None

    def __post_init__(self) -> None:
        self._state: dict[str, Any] | None = None

    # -- state --------------------------------------------------------------- #

    def _load(self) -> dict[str, Any]:
        if self._state is not None:
            return self._state
        self._state = {"active": {}}
        if self.state_path and self.state_path.is_file():
            try:
                loaded = json.loads(self.state_path.read_text(encoding="utf-8"))
                if isinstance(loaded.get("active"), dict):
                    self._state = loaded
            except (json.JSONDecodeError, OSError):
                # A corrupt alert state must not stop the collector; the worst
                # case is one duplicate alert.
                log.warning("unreadable alert state at %s, starting fresh", self.state_path)
        return self._state

    def _save(self) -> None:
        if not self.state_path or self._state is None:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_name(self.state_path.name + ".tmp")
        tmp.write_text(json.dumps(self._state, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.state_path)

    @property
    def active(self) -> dict[str, Any]:
        return self._load()["active"]

    # -- sending ------------------------------------------------------------- #

    def notify(
        self,
        key: str,
        title: str,
        message: str,
        *,
        severity: str = "error",
        details: dict[str, Any] | None = None,
    ) -> bool:
        """Raise an alert for ``key``. Returns True if a message was sent.

        Suppressed when the same key is already active and still inside its
        cooldown, so a persistent fault does not become a flood.
        """
        state = self._load()
        now = utcnow()
        existing = state["active"].get(key)
        if existing:
            try:
                last = datetime.fromisoformat(existing["last_sent"])
            except (KeyError, ValueError):
                last = None
            if last is not None and now - last < self.cooldown:
                log.debug("alert %s suppressed (within cooldown)", key)
                state["active"][key]["occurrences"] = existing.get("occurrences", 1) + 1
                self._save()
                return False

        state["active"][key] = {
            "first_seen": (existing or {}).get("first_seen", now.isoformat()),
            "last_sent": now.isoformat(),
            "occurrences": (existing or {}).get("occurrences", 0) + 1,
            "title": title,
        }
        self._save()
        return self._send(
            title=f"[{severity}] {title}",
            message=message,
            severity=severity,
            details={"key": key, **(details or {})},
        )

    def resolve(self, key: str, message: str = "") -> bool:
        """Clear ``key`` and announce the recovery, if it was active."""
        state = self._load()
        existing = state["active"].pop(key, None)
        self._save()
        if existing is None:
            return False
        return self._send(
            title=f"[resolved] {existing.get('title', key)}",
            message=message or f"{key} has recovered",
            severity="resolved",
            details={"key": key, "occurrences": existing.get("occurrences")},
        )

    def _send(self, *, title: str, message: str, severity: str, details: dict[str, Any]) -> bool:
        body = f"{title}\n{message}" if message else title
        if not self.webhook:
            log.info("alert (no webhook configured): %s", redact(body))
            return False

        payload = {
            # Slack and most self-hosted receivers read `text`; Discord reads
            # `content`. Sending both means one URL works either way.
            "text": body,
            "content": body,
            "title": title,
            "message": message,
            "severity": severity,
            "source": "tick-collector",
            "version": __version__,
            "hostname": self.hostname,
            "sent_at": utcnow().isoformat(),
            **details,
        }
        try:
            poster = self.post or _default_post
            poster(self.webhook, payload)
        except Exception as exc:
            # Never let a broken webhook take down a collection run.
            log.warning("alert delivery failed: %s", type(exc).__name__)
            return False
        log.info("alert sent: %s", redact(title))
        return True


def _default_post(url: str, payload: dict[str, Any]) -> None:
    with httpx.Client(timeout=TIMEOUT_SECONDS) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()


def from_config(config, state_dir: Path) -> Alerter:
    """Build an alerter from configuration."""
    return Alerter(webhook=config.alert_webhook, state_path=state_dir / "alerts.json")


# --------------------------------------------------------------------------- #
# What the collector alerts on
# --------------------------------------------------------------------------- #


def job_key(job: str) -> str:
    return f"job:{job}"


HEALTH_KEY = "health"


def report_job(alerter: Alerter, job: str, outcome: str, detail: str) -> None:
    """Alert on a job's hard failure, and announce its recovery.

    Soft flags deliberately do not alert. They are routine -- a real day flags
    ~8% of tickers for vwap -- and alerting on them would bury the failures that
    matter.
    """
    if outcome in ("hard_failure", "aborted"):
        alerter.notify(
            job_key(job),
            f"{job} {outcome}",
            detail,
            severity="error",
            details={"job": job, "outcome": outcome},
        )
    else:
        alerter.resolve(job_key(job), f"{job} completed {outcome}")


def report_health(alerter: Alerter, healthy: bool, reason: str) -> None:
    """Alert when the collector becomes unhealthy, and when it recovers."""
    if healthy:
        alerter.resolve(HEALTH_KEY, f"health restored: {reason}")
    else:
        alerter.notify(HEALTH_KEY, "collector unhealthy", reason, severity="error")
