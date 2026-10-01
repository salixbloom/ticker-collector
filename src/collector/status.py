"""``state/status.json``: the heartbeat and run summary.

Two consumers: the container health check (is the collector alive and was the
last run clean?) and, later, the dashboard. It is written with the same
write-temp-then-replace move the Parquet publisher uses, so a reader never sees
a half-written file.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import __version__
from .logging_setup import redact

STATUS_VERSION = 1


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass
class JobStatus:
    """Outcome of one job's last run."""

    job: str
    outcome: str  # ok | soft_flags | hard_failure | aborted | running
    started_at: str | None = None
    ended_at: str | None = None
    batches_published: int = 0
    batches_quarantined: int = 0
    rows: int = 0
    soft_flags: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    coverage: dict[str, int] = field(default_factory=dict)


@dataclass
class Status:
    """The whole status document."""

    status_version: int = STATUS_VERSION
    collector_version: str = __version__
    hostname: str = field(default_factory=socket.gethostname)
    updated_at: str = field(default_factory=lambda: utcnow().isoformat())
    started_at: str | None = None
    healthy: bool = True
    jobs: dict[str, JobStatus] = field(default_factory=dict)
    disk: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def to_json(self) -> str:
        payload = asdict(self)
        # Belt and braces: the status file is the most likely place for a stray
        # URL or error string to end up in front of a human.
        return redact(json.dumps(payload, indent=2, sort_keys=True, default=str))

    @property
    def has_hard_failure(self) -> bool:
        return any(job.outcome in ("hard_failure", "aborted") for job in self.jobs.values())


def write(status: Status, path: str | os.PathLike[str]) -> Path:
    """Atomically write the status document."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    status.updated_at = utcnow().isoformat()
    status.healthy = not status.has_hard_failure

    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(status.to_json())
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    return path


def read(path: str | os.PathLike[str]) -> Status | None:
    """Read the status document back, or ``None`` if absent or unreadable."""
    path = Path(path)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    jobs = {name: JobStatus(**value) for name, value in (payload.pop("jobs", {}) or {}).items()}
    known = set(Status.__dataclass_fields__)
    return Status(jobs=jobs, **{k: v for k, v in payload.items() if k in known})


def disk_usage(root: str | os.PathLike[str]) -> dict[str, Any]:
    """Free/used bytes for the data volume, so the dashboard can warn early.

    ``shutil.disk_usage`` reports the space available to an unprivileged user on
    POSIX, which is the number that matters here -- the collector does not run as
    root -- and it works on Windows too, so tests do not need a platform branch.
    """
    try:
        total, used, free = shutil.disk_usage(root)
    except OSError:
        return {}
    return {"total_bytes": total, "used_bytes": used, "free_bytes": free}
