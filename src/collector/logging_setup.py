"""Logging with secret redaction.

M0 requires that the API key never appears in logs, verified by grepping them. A
convention ("don't log the key") is not verifiable, so redaction is enforced
mechanically: every record passes through a filter that replaces any known secret
value, and any ``api_key``/``apiKey`` query parameter, with a placeholder. A URL
logged carelessly from inside the HTTP client is therefore still safe.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import re
import sys
from pathlib import Path
from typing import TextIO

from . import config as config_module

REDACTED = "***REDACTED***"

#: ``?apiKey=...`` / ``&api_key=...`` in a URL, whatever the value.
_QUERY_KEY_RE = re.compile(r"(?i)\b(api[_-]?key|token|secret)=([^&\s\"']+)")

#: ``Bearer <token>`` in a header dump.
_BEARER_RE = re.compile(r"(?i)\bBearer\s+([A-Za-z0-9._\-]{8,})")


def redact(text: str, secrets: list[str] | None = None) -> str:
    """Remove secret material from a string."""
    for value in secrets if secrets is not None else config_module.secret_values():
        if value:
            text = text.replace(value, REDACTED)
    text = _QUERY_KEY_RE.sub(lambda m: f"{m.group(1)}={REDACTED}", text)
    text = _BEARER_RE.sub(f"Bearer {REDACTED}", text)
    return text


class RedactingFilter(logging.Filter):
    """Redacts secrets from the message, its arguments and any exception text.

    Secrets are re-read from the environment on each record rather than captured
    once, so a key rotated at runtime is still redacted.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        secrets = config_module.secret_values()
        if isinstance(record.msg, str):
            record.msg = redact(record.msg, secrets)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    key: redact(str(value), secrets) if isinstance(value, str) else value
                    for key, value in record.args.items()
                }
            else:
                record.args = tuple(
                    redact(value, secrets) if isinstance(value, str) else value
                    for value in record.args
                )
        # Exception messages routinely carry the failing URL.
        if record.exc_info and record.exc_info[1] is not None:
            exc = record.exc_info[1]
            if exc.args and isinstance(exc.args[0], str):
                exc.args = (redact(exc.args[0], secrets), *exc.args[1:])
        return True


def setup(
    *,
    level: str | int | None = None,
    log_dir: Path | None = None,
    stream: TextIO | None = None,
    max_bytes: int = 16 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """Configure root logging: stderr always, a rotating file when given a dir.

    Logs rotate because the plan puts ``/data/logs`` on the Unraid cache drive,
    and an unbounded log file there eventually fills it.
    """
    resolved_level = level or os.environ.get("COLLECTOR_LOG_LEVEL", "INFO")
    root = logging.getLogger()
    root.setLevel(resolved_level)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    redacting = RedactingFilter()

    # stderr, not stdout: `collector status` and `collector info --json` write
    # machine-readable output to stdout, and interleaved log lines would corrupt
    # it. Docker captures both streams, so `docker logs` is unaffected.
    console = logging.StreamHandler(stream or sys.stderr)
    console.setFormatter(formatter)
    console.addFilter(redacting)
    root.addHandler(console)

    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        rotating = logging.handlers.RotatingFileHandler(
            log_dir / "collector.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        rotating.setFormatter(formatter)
        rotating.addFilter(redacting)
        root.addHandler(rotating)

    # urllib3 logs full URLs at DEBUG; the filter covers it, but the noise is not
    # useful at our level anyway.
    logging.getLogger("urllib3").setLevel(max(logging.INFO, logging.getLevelName(resolved_level)))
    return root
