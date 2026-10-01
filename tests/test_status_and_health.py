"""M0: the heartbeat, and the health check that reads it."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from collector import health
from collector import status as status_module
from collector.config import Config, HealthConfig
from collector.status import JobStatus, Status


@pytest.fixture
def config(tmp_path) -> Config:
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)
    return Config(data_root=tmp_path)


def status_path(config: Config):
    return config.data_root / "state" / "status.json"


# --------------------------------------------------------------------------- #
# status.json
# --------------------------------------------------------------------------- #


def test_write_then_read_round_trips(config):
    original = Status(started_at="2026-09-30T18:00:00+00:00")
    original.jobs["eod"] = JobStatus(
        job="eod", outcome="ok", batches_published=1, rows=10_412, coverage={"ok": 500}
    )
    status_module.write(original, status_path(config))

    reloaded = status_module.read(status_path(config))
    assert reloaded is not None
    assert reloaded.jobs["eod"].rows == 10_412
    assert reloaded.jobs["eod"].coverage == {"ok": 500}
    assert reloaded.healthy is True


def test_a_hard_failure_makes_the_document_unhealthy(config):
    current = Status()
    current.jobs["minute"] = JobStatus(job="minute", outcome="hard_failure", errors=["HTTP 500"])
    status_module.write(current, status_path(config))
    assert status_module.read(status_path(config)).healthy is False


def test_soft_flags_stay_healthy(config):
    current = Status()
    current.jobs["eod"] = JobStatus(
        job="eod", outcome="soft_flags", soft_flags=["ticker count 8% below median"]
    )
    status_module.write(current, status_path(config))
    assert status_module.read(status_path(config)).healthy is True


def test_the_write_is_atomic_and_leaves_no_temp_file(config):
    status_module.write(Status(), status_path(config))
    leftovers = [p.name for p in (config.data_root / "state").iterdir() if p.suffix == ".tmp"]
    assert leftovers == []


def test_secrets_are_redacted_from_the_status_file(config, monkeypatch):
    """An error string that happens to carry a URL must not leak the key."""
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-live-STATUSLEAK")
    current = Status()
    current.jobs["eod"] = JobStatus(
        job="eod",
        outcome="hard_failure",
        errors=["GET https://api.polygon.io/v2/aggs?apiKey=sk-live-STATUSLEAK -> 401"],
    )
    status_module.write(current, status_path(config))
    assert "STATUSLEAK" not in status_path(config).read_text(encoding="utf-8")


def test_reading_a_missing_or_corrupt_file_returns_none(config):
    assert status_module.read(status_path(config)) is None
    status_path(config).write_text("{ not json", encoding="utf-8")
    assert status_module.read(status_path(config)) is None


def test_disk_usage_reports_bytes(tmp_path):
    usage = status_module.disk_usage(tmp_path)
    assert usage["total_bytes"] > 0
    assert usage["free_bytes"] > 0


# --------------------------------------------------------------------------- #
# Health check
# --------------------------------------------------------------------------- #


def test_healthy_when_the_heartbeat_is_fresh(config):
    status_module.write(Status(), status_path(config))
    healthy, reason = health.evaluate(config)
    assert healthy, reason


def test_unhealthy_when_there_is_no_status_file(config):
    healthy, reason = health.evaluate(config)
    assert not healthy
    assert "no readable status file" in reason


def test_unhealthy_when_the_heartbeat_is_stale(config):
    stale = Status()
    status_module.write(stale, status_path(config))
    # Rewrite with an old timestamp; write() stamps the current time, so edit after.
    import json

    payload = json.loads(status_path(config).read_text(encoding="utf-8"))
    payload["updated_at"] = (datetime.now(UTC) - timedelta(hours=27)).isoformat()
    status_path(config).write_text(json.dumps(payload), encoding="utf-8")

    healthy, reason = health.evaluate(config)
    assert not healthy
    assert "old" in reason


def test_a_late_provider_update_does_not_trip_the_check(config):
    """The 26h window exists so one late day is not treated as a failure."""
    import json

    status_module.write(Status(), status_path(config))
    payload = json.loads(status_path(config).read_text(encoding="utf-8"))
    payload["updated_at"] = (datetime.now(UTC) - timedelta(hours=25)).isoformat()
    status_path(config).write_text(json.dumps(payload), encoding="utf-8")
    assert health.evaluate(config)[0] is True


def test_unhealthy_when_a_job_hard_failed(config):
    current = Status()
    current.jobs["eod"] = JobStatus(job="eod", outcome="hard_failure")
    status_module.write(current, status_path(config))

    healthy, reason = health.evaluate(config)
    assert not healthy
    assert "eod" in reason


def test_staleness_window_is_configurable(tmp_path):
    config = Config(data_root=tmp_path, health=HealthConfig(max_status_age_seconds=1))
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)
    import json

    status_module.write(Status(), tmp_path / "state" / "status.json")
    payload = json.loads((tmp_path / "state" / "status.json").read_text(encoding="utf-8"))
    payload["updated_at"] = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
    (tmp_path / "state" / "status.json").write_text(json.dumps(payload), encoding="utf-8")
    assert health.evaluate(config)[0] is False
