"""M0: the API key never appears in logs, verified by grepping them."""

from __future__ import annotations

import logging

import pytest

from collector.logging_setup import REDACTED, RedactingFilter, redact, setup

KEY = "sk-live-7f3a9b2c8d1e4f6a"


@pytest.fixture(autouse=True)
def api_key(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    monkeypatch.delenv("COLLECTOR_ALERT_WEBHOOK", raising=False)


def test_redacts_a_known_secret_value():
    assert KEY not in redact(f"calling https://api.polygon.io/v2/aggs?apiKey={KEY}")


@pytest.mark.parametrize(
    "text",
    [
        "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute?apiKey=UNKNOWN_BUT_SECRET",
        "GET /v3/reference/tickers?active=true&api_key=UNKNOWN_BUT_SECRET&limit=1000",
        "Authorization: Bearer UNKNOWN_BUT_SECRET",
        "token=UNKNOWN_BUT_SECRET",
    ],
)
def test_redacts_credentials_even_when_the_value_is_not_a_known_secret(text):
    """A rotated or mistyped key still must not land in the log."""
    cleaned = redact(text)
    assert "UNKNOWN_BUT_SECRET" not in cleaned
    assert REDACTED in cleaned


def test_redaction_keeps_the_rest_of_the_url_readable():
    cleaned = redact(f"GET /v2/aggs/ticker/AAPL/range/1/minute?apiKey={KEY}&limit=50000")
    assert "/v2/aggs/ticker/AAPL/range/1/minute" in cleaned
    assert "limit=50000" in cleaned


# --------------------------------------------------------------------------- #
# Through the logging machinery
# --------------------------------------------------------------------------- #


def grep_log(path) -> str:
    return path.read_text(encoding="utf-8")


def test_the_key_does_not_reach_the_log_file(tmp_path):
    log_dir = tmp_path / "logs"
    setup(level="DEBUG", log_dir=log_dir)
    log = logging.getLogger("collector.test")

    log.info("fetching %s", f"https://api.polygon.io/v2/aggs?apiKey={KEY}")
    log.debug("headers: {'Authorization': 'Bearer %s'}", KEY)
    log.warning("key is %s", KEY)
    log.error("request to https://api.polygon.io/v3/reference/tickers?apiKey=%s failed", KEY)
    logging.shutdown()

    contents = grep_log(log_dir / "collector.log")
    assert KEY not in contents
    assert "api.polygon.io" in contents  # the useful part is kept
    assert REDACTED in contents


def test_the_key_does_not_reach_an_exception_traceback_message(tmp_path):
    log_dir = tmp_path / "logs"
    setup(level="DEBUG", log_dir=log_dir)
    log = logging.getLogger("collector.test")

    try:
        raise RuntimeError(f"HTTP 401 for https://api.polygon.io/v2/aggs?apiKey={KEY}")
    except RuntimeError:
        log.exception("fetch failed")
    logging.shutdown()

    assert KEY not in grep_log(log_dir / "collector.log")


def test_filter_handles_dict_style_args():
    # LogRecord takes the mapping inside a one-tuple, the way logging.debug
    # ("a %(a)d", {"a": 1}) passes it.
    record = logging.LogRecord(
        name="t",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="%(url)s",
        args=({"url": f"https://x/?apiKey={KEY}"},),
        exc_info=None,
    )
    RedactingFilter().filter(record)
    assert KEY not in record.getMessage()


def test_filter_leaves_non_string_args_alone():
    record = logging.LogRecord(
        name="t",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="rows=%d bytes=%d",
        args=(10_412, 845_221),
        exc_info=None,
    )
    RedactingFilter().filter(record)
    assert record.getMessage() == "rows=10412 bytes=845221"


def test_log_file_rotates(tmp_path):
    log_dir = tmp_path / "logs"
    setup(level="INFO", log_dir=log_dir, max_bytes=2048, backup_count=2)
    log = logging.getLogger("collector.test")
    for i in range(500):
        log.info("a reasonably long log line to force rotation, iteration %d", i)
    logging.shutdown()

    files = sorted(p.name for p in log_dir.iterdir())
    assert "collector.log" in files
    assert any(name.startswith("collector.log.") for name in files)
    assert len(files) <= 3  # the live file plus backup_count
