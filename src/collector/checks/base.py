"""Check results: findings, severities, and the report a batch carries.

A check is **hard** (failure sends the batch to quarantine and nothing is
published) or **soft** (the batch publishes, flagged in the manifest and
``status.json`` for review). The split is the plan's, and it is load-bearing: a
hard check that fires on normal data would quarantine good days, and a soft check
on genuinely broken data would publish garbage.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class Severity(StrEnum):
    HARD = "hard"
    SOFT = "soft"


@dataclass(frozen=True)
class Finding:
    """One thing a check noticed."""

    check: str
    severity: Severity
    message: str
    #: Rows, tickers or values that triggered it. Capped when rendered, so a
    #: systemic failure does not write a megabyte of JSON into the manifest.
    samples: tuple[str, ...] = ()
    count: int = 0

    @property
    def is_hard(self) -> bool:
        return self.severity is Severity.HARD

    def to_dict(self, max_samples: int = 10) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "check": self.check,
            "severity": self.severity.value,
            "message": self.message,
        }
        if self.count:
            payload["count"] = self.count
        if self.samples:
            payload["samples"] = list(self.samples[:max_samples])
            if len(self.samples) > max_samples:
                payload["samples_truncated"] = len(self.samples) - max_samples
        return payload


@dataclass
class CheckReport:
    """Every finding for one batch, and the publish/quarantine verdict."""

    dataset: str
    batch_id: str
    findings: list[Finding] = field(default_factory=list)
    #: Checks that ran and found nothing, so a report proves what was examined
    #: rather than only what failed.
    passed: list[str] = field(default_factory=list)

    def add(self, finding: Finding | None) -> None:
        if finding is not None:
            self.findings.append(finding)

    def extend(self, findings: list[Finding]) -> None:
        self.findings.extend(findings)

    def record_pass(self, check: str) -> None:
        self.passed.append(check)

    @property
    def hard_failures(self) -> list[Finding]:
        return [f for f in self.findings if f.is_hard]

    @property
    def soft_flags(self) -> list[Finding]:
        return [f for f in self.findings if not f.is_hard]

    @property
    def ok(self) -> bool:
        """True when the batch may be published."""
        return not self.hard_failures

    @property
    def outcome(self) -> str:
        if self.hard_failures:
            return "hard_failure"
        if self.soft_flags:
            return "soft_flags"
        return "ok"

    def summary(self) -> str:
        if self.hard_failures:
            return "; ".join(f.message for f in self.hard_failures)
        if self.soft_flags:
            return "; ".join(f.message for f in self.soft_flags)
        return f"{len(self.passed)} check(s) passed"

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "batch_id": self.batch_id,
            "outcome": self.outcome,
            "checks_passed": sorted(self.passed),
            "hard": [f.to_dict() for f in self.hard_failures],
            "soft": [f.to_dict() for f in self.soft_flags],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True, default=str)


def hard(check: str, message: str, *, samples: list[str] | tuple[str, ...] = (), count: int = 0):
    return Finding(check, Severity.HARD, message, tuple(samples), count)


def soft(check: str, message: str, *, samples: list[str] | tuple[str, ...] = (), count: int = 0):
    return Finding(check, Severity.SOFT, message, tuple(samples), count)
