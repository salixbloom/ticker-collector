"""Container health check.

Unhealthy when ``status.json`` is missing, stale, or reports a hard failure. The
staleness window (26h by default) is deliberately longer than a day so that a
single late provider update does not restart a container that is working fine.
"""

from __future__ import annotations

from datetime import UTC, datetime

from . import status as status_module
from .config import Config


def evaluate(config: Config) -> tuple[bool, str]:
    """Return ``(healthy, reason)``."""
    path = config.data_root / "state" / "status.json"
    current = status_module.read(path)
    if current is None:
        return False, f"no readable status file at {path}"

    try:
        updated = datetime.fromisoformat(current.updated_at)
    except (TypeError, ValueError):
        return False, f"unparseable updated_at {current.updated_at!r}"
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=UTC)

    age = (datetime.now(UTC) - updated).total_seconds()
    limit = config.health.max_status_age_seconds
    if age > limit:
        return False, f"status is {age:.0f}s old (limit {limit}s)"

    failed = [
        name for name, job in current.jobs.items() if job.outcome in ("hard_failure", "aborted")
    ]
    if failed:
        return False, f"hard failure in job(s): {', '.join(sorted(failed))}"

    return True, f"ok, status {age:.0f}s old"
