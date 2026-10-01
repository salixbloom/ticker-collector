from __future__ import annotations

import json
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

from collector import manifest as manifest_module
from collector import status as status_module
from collector import storage
from collector.cli import main
from collector.scheduler import Scheduler

ET = ZoneInfo("America/New_York")


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    config = tmp_path / "collector.toml"
    config.write_text(
        f'[paths]\ndata_root = "{(tmp_path / "data").as_posix()}"\n', encoding="utf-8"
    )
    monkeypatch.delenv("COLLECTOR_DATA_ROOT", raising=False)
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    return config, tmp_path / "data"


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def test_init_creates_the_layout_manifest_and_heartbeat(workspace):
    config, data = workspace
    assert main(["--config", str(config), "init"]) == 0

    for name in ("published", "staging", "quarantine", "state", "logs"):
        assert (data / name).is_dir()
    assert (data / "state" / "manifest.sqlite").is_file()

    current = status_module.read(data / "state" / "status.json")
    assert current is not None
    assert current.started_at is not None
    assert current.disk["free_bytes"] > 0


def test_init_is_idempotent(workspace):
    config, _ = workspace
    assert main(["--config", str(config), "init"]) == 0
    assert main(["--config", str(config), "init"]) == 0


def test_init_clears_leftover_staging(workspace, eod_table):
    config, data = workspace
    main(["--config", str(config), "init"])
    paths = storage.DataPaths.from_root(data)
    storage.stage(
        eod_table, dataset="eod", paths=paths, batch_id="interrupted", filename="eod.parquet"
    )
    assert (paths.staging / "interrupted").exists()

    main(["--config", str(config), "init"])
    assert not (paths.staging / "interrupted").exists()


def test_healthcheck_fails_before_init_and_passes_after(workspace, capsys):
    config, _ = workspace
    assert main(["--config", str(config), "healthcheck"]) == 1
    main(["--config", str(config), "init"])
    assert main(["--config", str(config), "healthcheck"]) == 0
    assert "ok" in capsys.readouterr().out


def test_status_prints_the_document(workspace, capsys):
    config, _ = workspace
    main(["--config", str(config), "init"])
    assert main(["--config", str(config), "status"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status_version"] == status_module.STATUS_VERSION


def test_status_fails_cleanly_when_absent(workspace):
    config, _ = workspace
    assert main(["--config", str(config), "status"]) == 1


def test_info_reports_key_presence_but_never_the_key(workspace, capsys, monkeypatch):
    config, _ = workspace
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-live-CLILEAK")
    assert main(["--config", str(config), "info", "--json"]) == 0
    out = capsys.readouterr().out
    assert json.loads(out)["api_key_present"] is True
    assert "CLILEAK" not in out


def test_info_without_a_key(workspace, capsys):
    config, _ = workspace
    assert main(["--config", str(config), "info", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["api_key_present"] is False


def test_verify_passes_on_a_recorded_batch(workspace, eod_table, capsys):
    from datetime import date

    config, data = workspace
    main(["--config", str(config), "init"])
    paths = storage.DataPaths.from_root(data)

    staged = storage.stage(
        eod_table, dataset="eod", paths=paths, batch_id="b1", filename="eod_2026-09.parquet"
    )
    published = storage.publish(staged, paths.eod_path(date(2026, 9, 30)))
    with manifest_module.Manifest(paths.manifest_db) as man:
        man.record_batch(
            manifest_module.BatchRecord(
                batch_id="b1",
                dataset="eod",
                status=manifest_module.PUBLISHED,
                source="test",
                params={"date": "2026-09-30"},
                schema_version=staged.schema_version,
                ingested_at=manifest_module.utcnow(),
                partition_day=date(2026, 9, 30),
                path=str(published.relative_to(paths.root)),
                rows=staged.rows,
                bytes=staged.bytes,
                checksum_sha256=staged.checksum_sha256,
                published_at=manifest_module.utcnow(),
            )
        )

    assert main(["--config", str(config), "verify"]) == 0
    assert "match their manifest checksums" in capsys.readouterr().out

    published.write_bytes(published.read_bytes() + b"x")
    assert main(["--config", str(config), "verify"]) == 1


def test_a_bad_config_exits_2(tmp_path, capsys):
    bad = tmp_path / "bad.toml"
    bad.write_text("[storage]\ncompression_level = 99\n", encoding="utf-8")
    assert main(["--config", str(bad), "init"]) == 2
    assert "configuration error" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# Scheduler
# --------------------------------------------------------------------------- #


def test_a_job_is_due_at_its_slot_and_only_fires_once(monkeypatch):
    scheduler = Scheduler(tick_seconds=20)
    ran: list[str] = []
    job = scheduler.every_day_at("eod", lambda: ran.append("eod"), time(18, 30))

    at_slot = datetime(2026, 9, 30, 18, 30, 5, tzinfo=ET)
    assert scheduler._due(job, at_slot) is True
    scheduler._run(job, at_slot)
    assert ran == ["eod"]

    # Still inside the grace window -- must not run again.
    assert scheduler._due(job, at_slot + timedelta(seconds=30)) is False


def test_a_job_is_not_due_outside_its_window(monkeypatch):
    scheduler = Scheduler(tick_seconds=20)
    job = scheduler.every_day_at("eod", lambda: None, time(18, 30))
    assert scheduler._due(job, datetime(2026, 9, 30, 12, 0, tzinfo=ET)) is False
    assert scheduler._due(job, datetime(2026, 9, 30, 18, 35, tzinfo=ET)) is False


def test_a_tick_arriving_slightly_late_still_fires():
    """A job must not skip the day because a tick landed a few seconds late."""
    scheduler = Scheduler(tick_seconds=20)
    job = scheduler.every_day_at("eod", lambda: None, time(18, 30))
    assert scheduler._due(job, datetime(2026, 9, 30, 18, 30, 55, tzinfo=ET)) is True


def test_the_same_slot_fires_again_the_next_day():
    scheduler = Scheduler(tick_seconds=20)
    job = scheduler.every_day_at("eod", lambda: None, time(18, 30))
    today = datetime(2026, 9, 30, 18, 30, 1, tzinfo=ET)
    scheduler._run(job, today)
    assert scheduler._due(job, today + timedelta(days=1)) is True


def test_a_retry_slot_is_independent_of_the_main_slot():
    """eod runs at 18:30 and retries at 07:00; firing one must not consume the other."""
    scheduler = Scheduler(tick_seconds=20)
    job = scheduler.every_day_at("eod", lambda: None, time(18, 30), time(7, 0))
    evening = datetime(2026, 9, 30, 18, 30, 1, tzinfo=ET)
    scheduler._run(job, evening)
    assert scheduler._due(job, datetime(2026, 10, 1, 7, 0, 1, tzinfo=ET)) is True


def test_a_monthly_job_runs_once_per_month():
    scheduler = Scheduler(tick_seconds=20)
    job = scheduler.monthly_at("universe", lambda: None, time(20, 0))
    first = datetime(2026, 9, 1, 20, 0, 1, tzinfo=ET)
    assert scheduler._due(job, first) is True
    scheduler._run(job, first)
    assert scheduler._due(job, datetime(2026, 9, 15, 20, 0, 1, tzinfo=ET)) is False
    assert scheduler._due(job, datetime(2026, 10, 1, 20, 0, 1, tzinfo=ET)) is True


def test_a_failing_job_does_not_stop_the_scheduler(caplog):
    """Self-healing depends on there being a next run."""
    scheduler = Scheduler(tick_seconds=20)
    order: list[str] = []

    def boom():
        order.append("boom")
        raise RuntimeError("provider down")

    scheduler.every_day_at("broken", boom, time(18, 0))
    scheduler.every_day_at("fine", lambda: order.append("fine"), time(18, 0))

    now = datetime(2026, 9, 30, 18, 0, 1, tzinfo=ET)
    for job in scheduler.jobs:
        scheduler._run(job, now)
    assert order == ["boom", "fine"]
    assert "job broken failed" in caplog.text


def test_the_heartbeat_does_not_log_a_line_every_tick(caplog):
    """At one tick a minute, a success line would be ~1,400 lines of noise a day."""
    scheduler = Scheduler(tick_seconds=0.01)
    ticks: list[int] = []

    def beat():
        ticks.append(1)
        if len(ticks) >= 3:
            scheduler.stop()

    scheduler.set_heartbeat(beat)
    with caplog.at_level("INFO"):
        scheduler.run_forever()
    assert "heartbeat finished" not in caplog.text


def test_a_failing_heartbeat_is_still_logged(caplog):
    scheduler = Scheduler(tick_seconds=0.01)
    calls: list[int] = []

    def beat():
        calls.append(1)
        if len(calls) >= 2:
            scheduler.stop()
        raise OSError("disk full")

    scheduler.set_heartbeat(beat)
    scheduler.run_forever()
    assert "job heartbeat failed" in caplog.text


def test_run_forever_stops_on_request_and_writes_a_heartbeat():
    scheduler = Scheduler(tick_seconds=0.01)
    beats: list[int] = []

    def beat():
        beats.append(1)
        if len(beats) >= 3:
            scheduler.stop()

    scheduler.set_heartbeat(beat)
    scheduler.run_forever()
    assert len(beats) >= 3
    assert scheduler.stopping is True
