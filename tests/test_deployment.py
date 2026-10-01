"""The deployment surface: scheduled jobs, startup catch-up, doctor, permissions.

These cover what a Dockge/Unraid deploy actually depends on — that the container
collects rather than idling, that a misconfigured bind mount says so, and that
`doctor` reports the truth.
"""

from __future__ import annotations

import json
import os
import stat
from datetime import date, time
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest

from collector import jobs, storage
from collector import status as status_module
from collector.checks.trading_calendar import TradingCalendar
from collector.cli import _register_jobs, main
from collector.config import Config, SourceConfig
from collector.manifest import Manifest
from collector.ratelimit import RateLimiter
from collector.scheduler import Scheduler
from collector.source import Client
from collector.storage import StorageError

ET = ZoneInfo("America/New_York")
TODAY = date(2026, 9, 30)


# --------------------------------------------------------------------------- #
# The scheduled jobs
# --------------------------------------------------------------------------- #


def make_ctx(tmp_path, handler) -> jobs.Context:
    config = Config(
        data_root=tmp_path / "data",
        source=SourceConfig(base_url="https://api.massive.com", calls_per_minute=10_000),
    )
    paths = storage.DataPaths.from_root(config.data_root).ensure()
    client = Client(
        config=config,
        limiter=RateLimiter(10_000, sleep=lambda _s: None),
        http=httpx.Client(transport=httpx.MockTransport(handler), base_url=config.source.base_url),
        sleep=lambda _s: None,
        jitter=lambda: 0.5,
    )
    return jobs.Context(
        config=config,
        client=client,
        manifest=Manifest(paths.manifest_db),
        paths=paths,
        calendar=TradingCalendar(),
    )


def empty_api(request: httpx.Request) -> httpx.Response:
    """Answers every endpoint with a valid, empty, unadjusted response."""
    if "grouped" in request.url.path or "/aggs/" in request.url.path:
        return httpx.Response(200, json={"status": "OK", "adjusted": False, "results": []})
    return httpx.Response(200, json={"status": "OK", "results": []})


@pytest.fixture
def ctx(tmp_path):
    context = make_ctx(tmp_path, empty_api)
    yield context
    context.client.close()
    context.manifest.close()


def test_the_container_registers_every_built_job(ctx):
    """A deployed collector that registers nothing would look healthy and idle."""
    scheduler = Scheduler()
    _register_jobs(scheduler, ctx)
    assert [job.name for job in scheduler.jobs] == [
        "reference",
        "corporate_actions",
        "eod",
        "universe",
        "minute",
        "checks_report",
        "backup",
    ]


def test_the_jobs_run_at_the_times_the_plan_specifies(ctx):
    scheduler = Scheduler()
    _register_jobs(scheduler, ctx)
    slots = {job.name: job.at for job in scheduler.jobs}
    assert slots["reference"] == (time(18, 0),)
    assert slots["corporate_actions"] == (time(18, 15),)
    assert slots["eod"] == (time(18, 30), time(7, 0))  # evening plus a morning retry
    assert slots["universe"] == (time(19, 0),)
    assert slots["minute"] == (time(19, 15),)
    assert slots["checks_report"] == (time(20, 0),)
    assert slots["backup"] == (time(21, 0),)


def test_the_universe_is_rebuilt_monthly_not_daily(ctx):
    """Rebuilding daily would churn the minute universe and its coverage rows."""
    scheduler = Scheduler()
    _register_jobs(scheduler, ctx)
    by_name = {job.name: job for job in scheduler.jobs}
    assert by_name["universe"].monthly is True
    assert by_name["minute"].monthly is False


def test_the_universe_is_rebuilt_before_the_minute_job_runs(ctx):
    """So a newly rebuilt universe takes effect the same evening."""
    scheduler = Scheduler()
    _register_jobs(scheduler, ctx)
    at = {job.name: job.at[0] for job in scheduler.jobs}
    assert at["universe"] < at["minute"]


def test_corporate_actions_run_before_eod(ctx):
    """So the day's splits are on the books before the large-move check looks."""
    scheduler = Scheduler()
    _register_jobs(scheduler, ctx)
    order = [job.name for job in scheduler.jobs]
    assert order.index("corporate_actions") < order.index("eod")
    at = {job.name: job.at[0] for job in scheduler.jobs}
    assert at["corporate_actions"] < at["eod"]


def test_a_scheduled_job_writes_status_when_it_runs(ctx):
    scheduler = Scheduler()
    _register_jobs(scheduler, ctx)
    report = next(job for job in scheduler.jobs if job.name == "checks_report")
    report.fn()

    written = status_module.read(ctx.paths.status_file)
    assert written is not None
    assert "checks_report" in written.jobs


def test_a_failing_job_does_not_take_the_container_down(tmp_path, monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-test-deploy-123456")

    def broken(_request):
        return httpx.Response(500)

    ctx = make_ctx(tmp_path, broken)
    try:
        scheduler = Scheduler()
        _register_jobs(scheduler, ctx)
        reference = next(job for job in scheduler.jobs if job.name == "reference")
        reference.fn()  # must not raise
        written = status_module.read(ctx.paths.status_file)
        assert written is not None
        assert written.jobs["reference"].outcome == "hard_failure"
        assert written.healthy is False  # and the health check will say so
    finally:
        ctx.client.close()
        ctx.manifest.close()


def test_a_job_that_raises_still_reaches_status_json(ctx):
    """A revoked key must not leave the container reporting healthy and idle.

    Without this the failure is only in the logs, and the health check would not
    notice until status.json went stale 26 hours later.
    """
    scheduler = Scheduler()
    _register_jobs(scheduler, ctx)
    reference = next(job for job in scheduler.jobs if job.name == "reference")

    def explode(_ctx, **_kwargs):
        raise RuntimeError("no API key: revoked")

    import collector.jobs as jobs_module

    original = jobs_module.run_reference
    jobs_module.run_reference = explode
    try:
        scheduler_jobs = Scheduler()
        _register_jobs(scheduler_jobs, ctx)
        next(j for j in scheduler_jobs.jobs if j.name == "reference").fn()
    finally:
        jobs_module.run_reference = original

    written = status_module.read(ctx.paths.status_file)
    assert written is not None
    assert written.jobs["reference"].outcome == "hard_failure"
    assert "revoked" in " ".join(written.jobs["reference"].errors)
    assert written.healthy is False
    assert reference is not None


# --------------------------------------------------------------------------- #
# Startup catch-up
# --------------------------------------------------------------------------- #


def test_the_catch_up_pass_runs_every_job_once():
    """A container deployed at noon must not sit idle until its first slot."""
    clock = {"now": 0.0}
    scheduler = Scheduler(tick_seconds=0.01, clock=lambda: clock["now"])
    ran: list[str] = []

    for name in ("a", "b", "c"):
        scheduler.every_day_at(name, lambda n=name: ran.append(n), time(18, 0))

    def beat():
        clock["now"] += 1.0
        if len(ran) >= 3:
            scheduler.stop()

    scheduler.set_heartbeat(beat)
    scheduler.run_once_on_start(delay_seconds=2)
    scheduler.run_forever()

    assert ran == ["a", "b", "c"]


def test_the_catch_up_pass_runs_only_once():
    clock = {"now": 0.0}
    scheduler = Scheduler(tick_seconds=0.01, clock=lambda: clock["now"])
    ran: list[str] = []
    scheduler.every_day_at("a", lambda: ran.append("a"), time(18, 0))

    ticks = {"n": 0}

    def beat():
        ticks["n"] += 1
        clock["now"] += 5.0
        if ticks["n"] >= 6:
            scheduler.stop()

    scheduler.set_heartbeat(beat)
    scheduler.run_once_on_start(delay_seconds=1)
    scheduler.run_forever()

    assert ran == ["a"]  # not once per tick


def test_no_catch_up_happens_unless_asked():
    clock = {"now": 0.0}
    scheduler = Scheduler(tick_seconds=0.01, clock=lambda: clock["now"])
    ran: list[str] = []
    scheduler.every_day_at("a", lambda: ran.append("a"), time(18, 0))

    ticks = {"n": 0}

    def beat():
        ticks["n"] += 1
        clock["now"] += 100.0
        if ticks["n"] >= 3:
            scheduler.stop()

    scheduler.set_heartbeat(beat)
    scheduler.run_forever()
    assert ran == []


def test_the_catch_up_delay_is_respected():
    """Startup stays responsive: health comes up before collection begins."""
    clock = {"now": 0.0}
    scheduler = Scheduler(tick_seconds=0.01, clock=lambda: clock["now"])
    ran_at: list[float] = []
    scheduler.every_day_at("a", lambda: ran_at.append(clock["now"]), time(18, 0))

    def beat():
        clock["now"] += 10.0
        if ran_at:
            scheduler.stop()

    scheduler.set_heartbeat(beat)
    scheduler.run_once_on_start(delay_seconds=45)
    scheduler.run_forever()

    assert ran_at and ran_at[0] >= 45


# --------------------------------------------------------------------------- #
# checks_report
# --------------------------------------------------------------------------- #


def test_checks_report_summarises_coverage_without_calling_the_api(ctx):
    ctx.manifest.expect("eod", [date(2026, 9, 28), date(2026, 9, 29)])
    ctx.manifest.mark_coverage("eod", date(2026, 9, 28), "ok")
    calls_before = ctx.client.calls_made

    jobs.run_checks_report(ctx, today=TODAY)

    assert ctx.client.calls_made == calls_before  # no API traffic
    written = status_module.read(ctx.paths.status_file)
    assert written is not None
    notes = " ".join(written.notes)
    assert "eod" in notes
    assert "still owed" in notes


def test_checks_report_surfaces_quarantined_batches(ctx):
    from collector.manifest import QUARANTINED, BatchRecord, utcnow

    ctx.manifest.record_batch(
        BatchRecord(
            batch_id="bad",
            dataset="eod",
            status=QUARANTINED,
            source="test",
            params={},
            schema_version=2,
            ingested_at=utcnow(),
            partition_day=date(2026, 9, 29),
            reason="high < low",
        )
    )
    jobs.run_checks_report(ctx, today=TODAY)
    notes = " ".join(status_module.read(ctx.paths.status_file).notes)
    assert "quarantined" in notes


# --------------------------------------------------------------------------- #
# Permissions -- the usual Unraid bind-mount failure
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission semantics")
@pytest.mark.skipif(os.getuid() == 0, reason="root ignores the permission bits")
def test_an_unwritable_data_dir_fails_with_an_actionable_message(tmp_path):
    root = tmp_path / "data"
    root.mkdir()
    root.chmod(stat.S_IRUSR | stat.S_IXUSR)  # read-only, as a root-owned mount is
    try:
        with pytest.raises(StorageError) as caught:
            storage.DataPaths.from_root(root).ensure()
    finally:
        root.chmod(0o755)

    message = str(caught.value)
    assert "cannot write" in message
    # It must say what to do, not just what failed.
    assert "chown" in message
    assert "99:100" in message
    assert "APP_UID" in message


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission semantics")
@pytest.mark.skipif(os.getuid() == 0, reason="root ignores the permission bits")
def test_check_writable_catches_a_directory_that_exists_but_is_read_only(tmp_path):
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    paths.published.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        with pytest.raises(StorageError, match="cannot write"):
            paths.check_writable()
    finally:
        paths.published.chmod(0o755)


def test_check_writable_passes_on_a_normal_layout(tmp_path):
    storage.DataPaths.from_root(tmp_path / "data").check_writable()


# --------------------------------------------------------------------------- #
# doctor
# --------------------------------------------------------------------------- #


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    config = tmp_path / "collector.toml"
    config.write_text(
        f'[paths]\ndata_root = "{(tmp_path / "data").as_posix()}"\n', encoding="utf-8"
    )
    for name in ("COLLECTOR_DATA_ROOT", "COLLECTOR_CONFIG", "MASSIVE_API_KEY", "POLYGON_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    return config, tmp_path / "data"


def test_doctor_reports_a_missing_api_key_and_exits_nonzero(workspace, capsys):
    config, _ = workspace
    assert main(["--config", str(config), "doctor", "--offline"]) == 1
    out = capsys.readouterr().out
    assert "[FAIL] api key" in out
    assert "MASSIVE_API_KEY" in out
    # The things that are fine are still reported, so the output is a status page.
    assert "[  ok] permissions" in out


def test_doctor_passes_offline_with_a_key_set(workspace, capsys, monkeypatch):
    config, data = workspace
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-test-doctor-123456")
    assert main(["--config", str(config), "doctor", "--offline"]) == 0

    out = capsys.readouterr().out
    assert "All checks passed." in out
    assert "[FAIL]" not in out
    assert "api reachable" in out and "skipped" in out
    # It created the layout it checked.
    assert (data / "published").is_dir()


def test_doctor_never_prints_the_key(workspace, capsys, monkeypatch):
    config, _ = workspace
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-live-DOCTORLEAK")
    main(["--config", str(config), "doctor", "--offline"])
    assert "DOCTORLEAK" not in capsys.readouterr().out


def test_doctor_reports_the_manifest_schema_version(workspace, capsys, monkeypatch):
    from collector import manifest as manifest_module

    config, _ = workspace
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-test-doctor-123456")
    main(["--config", str(config), "doctor", "--offline"])
    assert f"schema v{manifest_module.SCHEMA_VERSION}" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# The shipped compose file and env template
# --------------------------------------------------------------------------- #

REPO = Path(__file__).resolve().parents[1]


def test_the_compose_file_builds_from_source_so_no_registry_is_needed():
    """Dockge runs `docker compose up -d`; a build: stanza makes that self-contained."""
    text = (REPO / "compose.yaml").read_text(encoding="utf-8")
    assert "build:" in text
    assert "target: app" in text
    assert "restart: unless-stopped" in text
    assert "healthcheck:" in text


def test_compose_drives_everything_from_env_so_only_one_file_is_edited():
    text = (REPO / "compose.yaml").read_text(encoding="utf-8")
    for variable in (
        "COLLECTOR_DATA_DIR",
        "APP_UID",
        "APP_GID",
        "TZ",
        "COLLECTOR_VERSION",
    ):
        assert f"${{{variable}" in text, f"{variable} should be settable from .env"


def test_every_compose_variable_is_documented_in_the_env_template():
    """An operator editing .env in Dockge should not have to read compose.yaml."""
    import re

    compose = (REPO / "compose.yaml").read_text(encoding="utf-8")
    template = (REPO / ".env.example").read_text(encoding="utf-8")
    referenced = set(re.findall(r"\$\{([A-Z_][A-Z0-9_]*)", compose))
    documented = set(re.findall(r"^#?\s*([A-Z_][A-Z0-9_]*)=", template, re.MULTILINE))
    assert referenced <= documented, f"undocumented: {sorted(referenced - documented)}"


def test_the_env_template_requires_only_the_api_key():
    template = (REPO / ".env.example").read_text(encoding="utf-8")
    assert "MASSIVE_API_KEY=" in template
    # Everything else ships with a usable default.
    assert "COLLECTOR_DATA_DIR=/mnt/user/quant-data" in template
    assert "APP_UID=99" in template
    assert "APP_GID=100" in template


def test_the_dev_compose_file_does_not_deploy_anything():
    """Keeping the test runner out of compose.yaml stops Dockge from showing it."""
    deploy = (REPO / "compose.yaml").read_text(encoding="utf-8")
    dev = (REPO / "compose.dev.yaml").read_text(encoding="utf-8")
    assert "tests" not in deploy
    assert "pytest" in dev
    assert "restart:" not in dev


def test_there_is_exactly_one_deployment_compose_file():
    """Two files (compose.yaml and docker-compose.yml) would be ambiguous."""
    candidates = sorted(p.name for p in REPO.glob("*compose*.y*ml") if "dev" not in p.name)
    assert candidates == ["compose.yaml"]


def test_the_shipped_config_enables_catch_up_on_start():
    """So a fresh deploy collects instead of idling until 18:00."""
    from collector.config import load

    config = load(REPO / "config" / "collector.toml")
    assert config.scheduler.catch_up_on_start is True
    assert config.scheduler.catch_up_delay_seconds > 0


def test_json_logs_are_bounded_so_the_cache_drive_cannot_fill():
    text = (REPO / "compose.yaml").read_text(encoding="utf-8")
    assert "max-size:" in text
    assert "max-file:" in text


def test_serve_without_an_api_key_stays_up_rather_than_crash_looping(workspace, monkeypatch):
    """Dockge would show a restart loop; an idle container with a warning is clearer."""
    config, data = workspace
    scheduler_calls: list[str] = []

    class FakeScheduler:
        def __init__(self, *_args, **_kwargs):
            pass

        def install_signal_handlers(self):
            scheduler_calls.append("signals")

        def set_heartbeat(self, _fn):
            scheduler_calls.append("heartbeat")

        def run_forever(self):
            scheduler_calls.append("ran")

        def run_once_on_start(self, **_kwargs):
            scheduler_calls.append("catch_up")

    monkeypatch.setattr("collector.cli.Scheduler", FakeScheduler)
    assert main(["--config", str(config), "serve"]) == 0
    assert "ran" in scheduler_calls
    assert "catch_up" not in scheduler_calls  # no key, so nothing to catch up
    assert json.loads((data / "state" / "status.json").read_text())["healthy"] is True
