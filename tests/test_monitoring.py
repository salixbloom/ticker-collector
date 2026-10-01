"""M9: alerting and backups. M10: the burn-in measurement.

M10's criterion is an observation over wall-clock time, so what is tested here is
the apparatus that measures it -- fed synthetic manifests that stand in for ten
days of running.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest

from collector import alerts, burnin, jobs, schemas, storage
from collector import backup as backup_module
from collector import manifest as manifest_module
from collector.alerts import Alerter
from collector.backup import BackupError
from collector.checks.trading_calendar import TradingCalendar
from collector.cli import _register_jobs, main
from collector.config import BackupConfig, Config
from collector.manifest import BatchRecord, Manifest
from collector.money import to_scaled
from collector.scheduler import Scheduler

CAL = TradingCalendar()
TODAY = date(2026, 10, 1)
INGESTED_AT = datetime(2026, 9, 30, 2, 0, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Alerting
# --------------------------------------------------------------------------- #


class Webhook:
    """Captures what would have been posted."""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.posts: list[dict] = []

    def __call__(self, url: str, payload: dict) -> None:
        if self.fail:
            raise RuntimeError("webhook refused")
        self.posts.append({"url": url, **payload})


@pytest.fixture
def hook() -> Webhook:
    return Webhook()


def make_alerter(tmp_path, hook, **kwargs) -> Alerter:
    return Alerter(
        webhook="https://hooks.example/abc",
        state_path=tmp_path / "alerts.json",
        post=hook,
        **kwargs,
    )


def test_a_hard_failure_sends_one_alert(tmp_path, hook):
    alerter = make_alerter(tmp_path, hook)
    assert alerter.notify("job:eod", "eod failed", "HTTP 503") is True
    assert len(hook.posts) == 1
    assert "eod failed" in hook.posts[0]["title"]
    assert "HTTP 503" in hook.posts[0]["text"]


def test_the_payload_suits_both_slack_and_discord(tmp_path, hook):
    """Slack reads `text`, Discord reads `content`; one URL should work either way."""
    make_alerter(tmp_path, hook).notify("k", "title", "body")
    post = hook.posts[0]
    assert post["text"] == post["content"]
    assert "title" in post["text"]
    assert "body" in post["text"]


def test_the_same_fault_does_not_alert_twice_inside_the_cooldown(tmp_path, hook):
    """An outage at 5-minute health intervals would otherwise send hundreds."""
    alerter = make_alerter(tmp_path, hook, cooldown=timedelta(hours=6))
    assert alerter.notify("job:eod", "eod failed", "down") is True
    for _ in range(20):
        assert alerter.notify("job:eod", "eod failed", "still down") is False
    assert len(hook.posts) == 1
    # But the repetition is counted, so the report can say how long it has run.
    assert alerter.active["job:eod"]["occurrences"] == 21


def test_a_fault_alerts_again_once_the_cooldown_passes(tmp_path, hook):
    alerter = make_alerter(tmp_path, hook, cooldown=timedelta(seconds=0))
    alerter.notify("job:eod", "eod failed", "down")
    assert alerter.notify("job:eod", "eod failed", "down") is True
    assert len(hook.posts) == 2


def test_dedup_state_survives_a_restart(tmp_path, hook):
    """A container restart must not re-announce everything that was already known."""
    first = make_alerter(tmp_path, hook, cooldown=timedelta(hours=6))
    first.notify("job:eod", "eod failed", "down")

    second = make_alerter(tmp_path, hook, cooldown=timedelta(hours=6))
    assert second.notify("job:eod", "eod failed", "down") is False
    assert len(hook.posts) == 1


def test_recovery_is_announced(tmp_path, hook):
    alerter = make_alerter(tmp_path, hook)
    alerter.notify("job:eod", "eod failed", "down")
    assert alerter.resolve("job:eod", "back to normal") is True
    assert "resolved" in hook.posts[-1]["title"]
    assert alerter.active == {}


def test_resolving_something_that_never_fired_says_nothing(tmp_path, hook):
    alerter = make_alerter(tmp_path, hook)
    assert alerter.resolve("job:eod") is False
    assert hook.posts == []


def test_a_broken_webhook_never_breaks_the_collector(tmp_path):
    alerter = make_alerter(tmp_path, Webhook(fail=True))
    assert alerter.notify("job:eod", "eod failed", "down") is False  # no exception


def test_with_no_webhook_configured_everything_still_works(tmp_path):
    alerter = Alerter(webhook=None, state_path=tmp_path / "alerts.json")
    assert alerter.notify("job:eod", "eod failed", "down") is False
    assert alerter.resolve("job:eod") is False


def test_a_corrupt_alert_state_is_recovered_from(tmp_path, hook):
    (tmp_path / "alerts.json").write_text("{not json", encoding="utf-8")
    alerter = make_alerter(tmp_path, hook)
    assert alerter.notify("k", "t", "m") is True


def test_the_webhook_url_is_not_leaked_into_logs(tmp_path, hook, caplog, monkeypatch):
    monkeypatch.setenv("COLLECTOR_ALERT_WEBHOOK", "https://hooks.example/SECRETPATH")
    alerter = Alerter(
        webhook="https://hooks.example/SECRETPATH",
        state_path=tmp_path / "alerts.json",
        post=hook,
    )
    with caplog.at_level("INFO"):
        alerter.notify("k", "t", "https://hooks.example/SECRETPATH")
    assert "SECRETPATH" not in caplog.text


def test_soft_flags_do_not_alert(tmp_path, hook):
    """A real day flags ~8% of tickers; alerting on that buries the real failures."""
    alerter = make_alerter(tmp_path, hook)
    alerts.report_job(alerter, "eod", "soft_flags", "1060 rows have vwap out of range")
    assert hook.posts == []


def test_a_hard_failure_then_a_clean_run_alerts_then_resolves(tmp_path, hook):
    alerter = make_alerter(tmp_path, hook)
    alerts.report_job(alerter, "eod", "hard_failure", "HTTP 503")
    alerts.report_job(alerter, "eod", "ok", "")
    assert len(hook.posts) == 2
    assert "resolved" in hook.posts[-1]["title"]


def test_health_transitions_alert_and_resolve(tmp_path, hook):
    alerter = make_alerter(tmp_path, hook)
    alerts.report_health(alerter, False, "status is 99999s old")
    alerts.report_health(alerter, True, "ok, status 10s old")
    assert len(hook.posts) == 2
    assert "unhealthy" in hook.posts[0]["title"]


# --------------------------------------------------------------------------- #
# M9: the simulated outage
# --------------------------------------------------------------------------- #


def test_an_outage_alerts_and_the_next_run_fills_the_gap(tmp_path, hook, monkeypatch):
    """M9's definition of done, end to end through the scheduled job wrapper."""
    monkeypatch.setenv("MASSIVE_API_KEY", "sk-test-outage-123456")
    import httpx

    from collector.config import CollectionConfig, SourceConfig
    from collector.ratelimit import RateLimiter
    from collector.source import Client

    state = {"down": True}

    def api(request: httpx.Request) -> httpx.Response:
        if state["down"]:
            return httpx.Response(503)
        if "grouped" in request.url.path:
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "adjusted": False,
                    "results": [
                        {"T": "AAA", "o": 1.0, "h": 1.5, "l": 0.5, "c": 1.2, "v": 100.5, "n": 3}
                    ],
                },
            )
        return httpx.Response(200, json={"status": "OK", "results": []})

    config = Config(
        data_root=tmp_path / "data",
        source=SourceConfig(
            base_url="https://api.massive.com", calls_per_minute=10_000, max_retries=0
        ),
        collection=CollectionConfig(backfill_days=0),
    )
    paths = storage.DataPaths.from_root(config.data_root).ensure()
    client = Client(
        config=config,
        limiter=RateLimiter(10_000, sleep=lambda _s: None),
        http=httpx.Client(transport=httpx.MockTransport(api), base_url=config.source.base_url),
        sleep=lambda _s: None,
        jitter=lambda: 0.5,
    )
    ctx = jobs.Context(
        config=config,
        client=client,
        manifest=Manifest(paths.manifest_db),
        paths=paths,
        calendar=CAL,
    )
    alerter = make_alerter(tmp_path, hook)

    try:
        scheduler = Scheduler()
        _register_jobs(scheduler, ctx, alerter)
        eod_job = next(j for j in scheduler.jobs if j.name == "eod")

        # --- the outage ---
        eod_job.fn()
        assert hook.posts, "an outage must alert"
        assert "eod" in hook.posts[0]["title"]
        owed = ctx.manifest.gaps("eod")
        assert owed, "the day must still be owed after an outage"

        # --- the outage ends; nothing is done by hand ---
        state["down"] = False
        hook.posts.clear()
        eod_job.fn()

        assert ctx.manifest.gaps("eod") == [], "the next run must fill the gap"
        assert any("resolved" in p["title"] for p in hook.posts), "recovery must be announced"
    finally:
        ctx.client.close()
        ctx.manifest.close()


# --------------------------------------------------------------------------- #
# M9: backup and restore
# --------------------------------------------------------------------------- #


def populate(paths: storage.DataPaths, days: list[date]) -> None:
    """Publish a little EOD data and record it, so a backup has substance."""
    with Manifest(paths.manifest_db) as man:
        for day in days:
            table = pa.table(
                {
                    "ticker": ["AAA", "BBB"],
                    "date": [day, day],
                    "open": [to_scaled("1.0")] * 2,
                    "high": [to_scaled("2.0")] * 2,
                    "low": [to_scaled("0.5")] * 2,
                    "close": [to_scaled("1.5")] * 2,
                    "vwap": [to_scaled("1.4")] * 2,
                    "volume": [to_scaled("100.5")] * 2,
                    "transactions": [3, 4],
                    "ingested_at": [INGESTED_AT] * 2,
                },
                schema=schemas.EOD_SCHEMA,
            )
            destination = paths.eod_path(day)
            existing = storage.read_parquet(destination) if destination.exists() else None
            merged = storage.merge_rows(existing, table, "eod")
            staged = storage.stage(
                merged, dataset="eod", paths=paths, batch_id=f"b-{day}", filename="eod.parquet"
            )
            published = storage.publish(staged, destination)
            man.expect("eod", [day])
            man.mark_coverage("eod", day, manifest_module.OK)
            man.record_batch(
                BatchRecord(
                    batch_id=f"eod-{day.isoformat()}-r1",
                    dataset="eod",
                    status=manifest_module.PUBLISHED,
                    source="test",
                    params={"date": day.isoformat()},
                    schema_version=schemas.SCHEMA_VERSION,
                    ingested_at=manifest_module.utcnow(),
                    partition_day=day,
                    path=str(published.relative_to(paths.root)),
                    rows=staged.rows,
                    source_rows=2,
                    bytes=staged.bytes,
                    checksum_sha256=staged.checksum_sha256,
                    content_sha256=storage.content_sha256(
                        storage.read_parquet(published).filter(
                            __import__("pyarrow.compute", fromlist=["equal"]).equal(
                                storage.read_parquet(published).column("date"), day
                            )
                        ),
                        "eod",
                    ),
                    published_at=manifest_module.utcnow(),
                )
            )
        status_path = paths.status_file
        status_path.write_text('{"healthy": true}', encoding="utf-8")


@pytest.fixture
def populated(tmp_path) -> storage.DataPaths:
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    populate(paths, CAL.recent_trading_days(date(2026, 9, 29), 3))
    return paths


def test_a_backup_records_and_verifies_every_file(populated, tmp_path):
    info = backup_module.create(populated, tmp_path / "backups")
    assert info.files > 1
    assert info.includes_data
    assert backup_module.verify(info.path) == []

    recorded = json.loads((info.path / backup_module.MANIFEST_NAME).read_text())
    assert recorded["manifest_schema_version"] == manifest_module.SCHEMA_VERSION
    assert any(name.endswith(".parquet") for name in recorded["checksums"])


def test_the_manifest_snapshot_is_a_usable_database(populated, tmp_path):
    """Copied via SQLite's backup API, so a live WAL database cannot be torn."""
    info = backup_module.create(populated, tmp_path / "backups")
    conn = sqlite3.connect(info.path / backup_module.DB_NAME)
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        count = conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
        assert count == 3
    finally:
        conn.close()


def test_a_backup_taken_while_the_manifest_is_open_is_still_consistent(populated, tmp_path):
    """The real case: the collector is running when the backup job fires."""
    with Manifest(populated.manifest_db) as live:
        live.expect("eod", [date(2026, 10, 1)])  # an open connection, mid-write
        info = backup_module.create(populated, tmp_path / "backups")

    assert backup_module.verify(info.path) == []
    conn = sqlite3.connect(info.path / backup_module.DB_NAME)
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def test_a_state_only_backup_skips_published_data(populated, tmp_path):
    info = backup_module.create(populated, tmp_path / "backups", include_data=False)
    assert not info.includes_data
    recorded = json.loads((info.path / backup_module.MANIFEST_NAME).read_text())
    assert not any(name.endswith(".parquet") for name in recorded["checksums"])
    assert backup_module.DB_NAME in recorded["checksums"]


def test_verify_detects_a_tampered_backup(populated, tmp_path):
    info = backup_module.create(populated, tmp_path / "backups")
    target = next(info.path.rglob("*.parquet"))
    target.write_bytes(target.read_bytes() + b"x")

    problems = backup_module.verify(info.path)
    assert problems and "!=" in problems[0]


def test_verify_detects_a_missing_file(populated, tmp_path):
    info = backup_module.create(populated, tmp_path / "backups")
    next(info.path.rglob("*.parquet")).unlink()
    assert any("missing" in p for p in backup_module.verify(info.path))


def test_verify_detects_an_unrecorded_extra_file(populated, tmp_path):
    info = backup_module.create(populated, tmp_path / "backups")
    (info.path / "surprise.parquet").write_bytes(b"x")
    assert any("not recorded" in p for p in backup_module.verify(info.path))


def test_a_directory_that_is_not_a_backup_is_rejected(tmp_path):
    (tmp_path / "random").mkdir()
    assert backup_module.verify(tmp_path / "random")


# --- restore ------------------------------------------------------------- #


def test_restore_rebuilds_the_data_root(populated, tmp_path):
    """M9's definition of done: a restore from backup is tested."""
    info = backup_module.create(populated, tmp_path / "backups")
    before = sorted(
        p.relative_to(populated.root).as_posix() for p in populated.published.rglob("*.parquet")
    )
    checksums = {p.name: storage.sha256_file(p) for p in populated.published.rglob("*.parquet")}

    fresh = storage.DataPaths.from_root(tmp_path / "restored").ensure()
    restored = backup_module.restore(info.path, fresh)

    assert restored.files == info.files
    after = sorted(p.relative_to(fresh.root).as_posix() for p in fresh.published.rglob("*.parquet"))
    assert after == before
    for path in fresh.published.rglob("*.parquet"):
        assert storage.sha256_file(path) == checksums[path.name]


def test_a_restored_manifest_still_verifies_against_the_restored_data(populated, tmp_path):
    """The whole point: after a restore, `verify` can confirm it rather than hope."""
    info = backup_module.create(populated, tmp_path / "backups")
    fresh = storage.DataPaths.from_root(tmp_path / "restored").ensure()
    backup_module.restore(info.path, fresh)

    with Manifest(fresh.manifest_db) as man:
        assert man.verify_checksums(fresh.root) == []
        assert len(man.published_batches("eod")) == 3


def test_restore_refuses_to_overwrite_existing_data(populated, tmp_path):
    """A mistyped command must not destroy the archive it was meant to protect."""
    info = backup_module.create(populated, tmp_path / "backups")
    with pytest.raises(BackupError, match="already holds"):
        backup_module.restore(info.path, populated)


def test_restore_proceeds_when_forced(populated, tmp_path):
    info = backup_module.create(populated, tmp_path / "backups")
    assert backup_module.restore(info.path, populated, force=True).files == info.files


def test_restore_refuses_a_corrupt_backup(populated, tmp_path):
    info = backup_module.create(populated, tmp_path / "backups")
    target = next(info.path.rglob("*.parquet"))
    target.write_bytes(b"corrupt")

    fresh = storage.DataPaths.from_root(tmp_path / "restored").ensure()
    with pytest.raises(BackupError, match="refusing to restore"):
        backup_module.restore(info.path, fresh)
    assert not list(fresh.published.rglob("*.parquet"))


def test_restore_clears_stale_wal_files(populated, tmp_path):
    """A leftover WAL from the previous database would contradict the restored one."""
    info = backup_module.create(populated, tmp_path / "backups")
    fresh = storage.DataPaths.from_root(tmp_path / "restored").ensure()
    wal = fresh.manifest_db.with_name(fresh.manifest_db.name + "-wal")
    wal.parent.mkdir(parents=True, exist_ok=True)
    wal.write_bytes(b"stale")

    backup_module.restore(info.path, fresh)
    assert not wal.exists()


# --- listing and pruning ------------------------------------------------- #


def test_backups_are_listed_newest_first_and_pruned(populated, tmp_path):
    destination = tmp_path / "backups"
    for index in range(4):
        backup_module.create(populated, destination, label=f"n{index}")

    listed = backup_module.list_backups(destination)
    assert len(listed) == 4
    assert [i.path.name for i in listed] == sorted((i.path.name for i in listed), reverse=True)

    removed = backup_module.prune(destination, keep=2)
    assert len(removed) == 2
    assert len(backup_module.list_backups(destination)) == 2


def test_pruning_keeps_the_newest(populated, tmp_path):
    destination = tmp_path / "backups"
    for index in range(3):
        backup_module.create(populated, destination, label=f"n{index}")
    newest = backup_module.list_backups(destination)[0].path.name
    backup_module.prune(destination, keep=1)
    assert [i.path.name for i in backup_module.list_backups(destination)] == [newest]


def test_prune_refuses_to_keep_nothing(tmp_path):
    with pytest.raises(BackupError):
        backup_module.prune(tmp_path, keep=0)


# --- the scheduled backup job -------------------------------------------- #


def make_ctx(paths: storage.DataPaths, **backup_kwargs) -> jobs.Context:
    config = Config(data_root=paths.root, backup=BackupConfig(**backup_kwargs))
    return jobs.Context(
        config=config,
        client=None,  # the backup job makes no API calls
        manifest=Manifest(paths.manifest_db),
        paths=paths,
        calendar=CAL,
    )


def test_the_backup_job_writes_and_verifies(populated, tmp_path):
    ctx = make_ctx(populated, directory=str(tmp_path / "backups"))
    try:
        result = jobs.run_backup(ctx, today=TODAY)
        assert result.outcome == "ok"
        assert result.published == 1
        listed = backup_module.list_backups(tmp_path / "backups")
        assert len(listed) == 1
        assert backup_module.verify(listed[0].path) == []
    finally:
        ctx.manifest.close()


def test_the_backup_job_makes_no_api_calls(populated, tmp_path):
    """It is pure disk work, so it is safe to schedule alongside everything else."""
    ctx = make_ctx(populated, directory=str(tmp_path / "backups"))
    try:
        jobs.run_backup(ctx, today=TODAY)  # client is None; would raise if used
    finally:
        ctx.manifest.close()


def test_the_backup_job_is_a_no_op_when_unconfigured(populated):
    ctx = make_ctx(populated, directory="")
    try:
        result = jobs.run_backup(ctx, today=TODAY)
        assert result.outcome == "ok"
        assert result.published == 0
    finally:
        ctx.manifest.close()


def test_the_backup_job_prunes_to_the_configured_count(populated, tmp_path):
    ctx = make_ctx(populated, directory=str(tmp_path / "backups"), keep=2)
    try:
        for _ in range(4):
            jobs.run_backup(ctx, today=TODAY)
        assert len(backup_module.list_backups(tmp_path / "backups")) <= 2
    finally:
        ctx.manifest.close()


def test_a_failing_backup_is_reported_not_raised(populated, tmp_path):
    ctx = make_ctx(populated, directory=str(tmp_path / "nope" / "\0bad"))
    try:
        result = jobs.run_backup(ctx, today=TODAY)
        assert result.outcome == "hard_failure"
        assert result.errors
    finally:
        ctx.manifest.close()


# --------------------------------------------------------------------------- #
# M10: the burn-in measurement
# --------------------------------------------------------------------------- #


def burnin_report(paths: storage.DataPaths, **kwargs):
    with Manifest(paths.manifest_db) as man:
        return burnin.evaluate(man, paths, CAL, today=TODAY, **kwargs)


def test_ten_clean_days_pass_the_burn_in(tmp_path):
    """The apparatus M10 is measured with, fed ten clean days."""
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    days = CAL.recent_trading_days(date(2026, 9, 30), 10)
    populate(paths, days)

    report = burnin_report(paths, required=10, window=10)
    assert report.consecutive_clean == 10
    assert report.passed
    assert report.total_rows > 0
    assert [d for d in report.days if not d.clean] == []


def test_one_owed_day_breaks_the_streak(tmp_path):
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    days = CAL.recent_trading_days(date(2026, 9, 30), 10)
    populate(paths, days)
    with Manifest(paths.manifest_db) as man:
        man.mark_coverage("eod", days[-3], manifest_module.FAILED, error="HTTP 503")

    report = burnin_report(paths, required=10, window=10)
    assert not report.passed
    # Counted back from the most recent day, so the streak stops at the fault.
    assert report.consecutive_clean == 2
    assert any("still owed" in r for d in report.days for r in d.reasons)


def test_a_quarantined_batch_breaks_the_streak(tmp_path):
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    days = CAL.recent_trading_days(date(2026, 9, 30), 10)
    populate(paths, days)
    with Manifest(paths.manifest_db) as man:
        man.record_batch(
            BatchRecord(
                batch_id="bad",
                dataset="eod",
                status=manifest_module.QUARANTINED,
                source="test",
                params={},
                schema_version=schemas.SCHEMA_VERSION,
                ingested_at=manifest_module.utcnow(),
                partition_day=days[-1],
                reason="high < low",
            )
        )
    report = burnin_report(paths, required=10, window=10)
    assert not report.passed
    assert report.consecutive_clean == 0
    assert any("quarantined" in r for d in report.days for r in d.reasons)


def test_soft_flags_do_not_break_the_streak(tmp_path):
    """A real day flags ~8% of EOD tickers for vwap; that is data, not a fault."""
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    days = CAL.recent_trading_days(date(2026, 9, 30), 10)
    populate(paths, days)
    with Manifest(paths.manifest_db) as man:
        for day in days:
            man.conn.execute(
                "UPDATE batches SET reason = ? WHERE partition_day = ?",
                ("1060 row(s) have vwap outside [low, high]", day.isoformat()),
            )

    report = burnin_report(paths, required=10, window=10)
    assert report.passed
    assert report.soft_flag_rate == 1.0


def test_an_explained_absence_does_not_break_the_streak(tmp_path):
    """A `skipped` ticker-day has a recorded reason, so it is not an unexplained gap."""
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    days = CAL.recent_trading_days(date(2026, 9, 30), 10)
    populate(paths, days)
    with Manifest(paths.manifest_db) as man:
        man.expect("minute", [days[-1]], tickers=["QUIET"])
        man.mark_coverage(
            "minute", days[-1], manifest_module.SKIPPED, ticker="QUIET", error="no bars"
        )
    report = burnin_report(paths, required=10, window=10)
    assert report.passed


def test_a_checksum_problem_fails_the_burn_in(tmp_path):
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    days = CAL.recent_trading_days(date(2026, 9, 30), 10)
    populate(paths, days)
    target = next(paths.published.rglob("*.parquet"))
    target.write_bytes(target.read_bytes() + b"tampered")

    report = burnin_report(paths, required=10, window=10)
    assert not report.passed
    assert report.checksum_problems


def test_a_fresh_install_is_in_progress_not_passing(tmp_path):
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    report = burnin_report(paths, required=10, window=10)
    assert not report.passed
    assert report.consecutive_clean == 0


def test_the_render_names_the_fault(tmp_path):
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    days = CAL.recent_trading_days(date(2026, 9, 30), 10)
    populate(paths, days)
    with Manifest(paths.manifest_db) as man:
        man.mark_coverage("eod", days[-1], manifest_module.FAILED, error="HTTP 503")

    text = burnin.render(burnin_report(paths, required=10, window=10))
    assert "IN PROGRESS" in text
    assert days[-1].isoformat() in text
    assert "still owed" in text
    assert "more consecutive clean trading day(s) needed" in text


def test_the_journal_write_up_covers_what_m10_asks_for(tmp_path):
    """Storage size, check flag rates, issues found."""
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    days = CAL.recent_trading_days(date(2026, 9, 30), 10)
    populate(paths, days)

    entry = burnin.journal_entry(burnin_report(paths, required=10, window=10), paths)
    assert "# Burn-in write-up" in entry
    assert "## Storage" in entry
    assert "## Check flags" in entry
    assert "## Issues found" in entry
    assert "eod" in entry
    assert "MB" in entry


def test_the_json_form_is_machine_readable(tmp_path):
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    populate(paths, CAL.recent_trading_days(date(2026, 9, 30), 10))
    payload = json.loads(burnin.to_json(burnin_report(paths, required=10, window=10)))
    assert payload["passed"] is True
    assert payload["consecutive_clean_days"] == 10
    assert payload["faults"] == []


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    config = tmp_path / "collector.toml"
    config.write_text(
        f'[paths]\ndata_root = "{(tmp_path / "data").as_posix()}"\n', encoding="utf-8"
    )
    for name in ("COLLECTOR_DATA_ROOT", "COLLECTOR_CONFIG", "COLLECTOR_ALERT_WEBHOOK"):
        monkeypatch.delenv(name, raising=False)
    return config, tmp_path


def test_cli_backup_then_verify_then_restore(workspace, capsys):
    config, tmp_path = workspace
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    populate(paths, CAL.recent_trading_days(date(2026, 9, 29), 2))
    destination = tmp_path / "backups"

    assert main(["--config", str(config), "backup", "--to", str(destination)]) == 0
    assert "verified" in capsys.readouterr().out

    taken = backup_module.list_backups(destination)[0].path
    assert main(["--config", str(config), "verify-backup", str(taken)]) == 0
    assert main(["--config", str(config), "backups", "--in", str(destination)]) == 0
    assert taken.name in capsys.readouterr().out

    # Restoring over live data needs --force.
    assert main(["--config", str(config), "restore", str(taken)]) == 1
    assert "already holds" in capsys.readouterr().err
    assert main(["--config", str(config), "restore", str(taken), "--force"]) == 0
    assert "restored" in capsys.readouterr().out


def test_cli_backup_without_a_destination_explains_itself(workspace, capsys):
    config, _ = workspace
    assert main(["--config", str(config), "backup"]) == 2
    assert "no backup directory" in capsys.readouterr().err


def test_cli_verify_backup_fails_on_a_tampered_backup(workspace, capsys):
    config, tmp_path = workspace
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    populate(paths, [date(2026, 9, 29)])
    destination = tmp_path / "backups"
    main(["--config", str(config), "backup", "--to", str(destination)])
    capsys.readouterr()

    taken = backup_module.list_backups(destination)[0].path
    target = next(taken.rglob("*.parquet"))
    target.write_bytes(b"corrupt")
    assert main(["--config", str(config), "verify-backup", str(taken)]) == 1
    assert "did NOT verify" in capsys.readouterr().err


def test_cli_burnin_reports_progress_and_exits_nonzero_until_it_passes(workspace, capsys):
    config, tmp_path = workspace
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    populate(paths, CAL.recent_trading_days(date(2026, 9, 30), 3))

    assert main(["--config", str(config), "burnin", "--today", "2026-10-01"]) == 1
    out = capsys.readouterr().out
    assert "IN PROGRESS" in out
    assert "/10" in out


def test_cli_burnin_passes_with_enough_clean_days(workspace, capsys):
    config, tmp_path = workspace
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    populate(paths, CAL.recent_trading_days(date(2026, 9, 30), 10))

    code = main(["--config", str(config), "burnin", "--today", "2026-10-01", "--window", "10"])
    assert code == 0
    assert "PASS" in capsys.readouterr().out


def test_cli_burnin_writes_the_journal(workspace, capsys, tmp_path):
    config, workdir = workspace
    paths = storage.DataPaths.from_root(workdir / "data").ensure()
    populate(paths, CAL.recent_trading_days(date(2026, 9, 30), 10))
    journal = workdir / "journal" / "burnin.md"

    main(
        [
            "--config",
            str(config),
            "burnin",
            "--today",
            "2026-10-01",
            "--window",
            "10",
            "--journal",
            str(journal),
        ]
    )
    assert journal.is_file()
    assert "# Burn-in write-up" in journal.read_text(encoding="utf-8")


def test_cli_alert_test_explains_itself_with_no_webhook(workspace, capsys):
    config, _ = workspace
    assert main(["--config", str(config), "alert-test"]) == 2
    assert "COLLECTOR_ALERT_WEBHOOK" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# The scheduled backup job is registered
# --------------------------------------------------------------------------- #


def test_the_backup_job_is_scheduled_after_the_days_collection(populated):
    from datetime import time

    ctx = make_ctx(populated, directory="")
    try:
        scheduler = Scheduler()
        _register_jobs(scheduler, ctx)
        by_name = {j.name: j for j in scheduler.jobs}
        assert "backup" in by_name
        assert by_name["backup"].at == (time(21, 0),)
        # After everything that writes data, so a backup captures a complete day.
        assert by_name["backup"].at[0] > by_name["minute"].at[0]
        assert by_name["backup"].at[0] > by_name["checks_report"].at[0]
    finally:
        ctx.manifest.close()


def test_a_re_collected_day_is_not_counted_twice(tmp_path):
    """Several published revisions of one day must not multiply its row count.

    A day re-collected after a correction has r1, r2, ...; only the current one
    describes what is on disk.
    """
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    days = CAL.recent_trading_days(date(2026, 9, 30), 10)
    populate(paths, days)
    target = days[-1]

    before = burnin_report(paths, required=10, window=10)
    rows_before = next(d.rows for d in before.days if d.day == target)

    # A second published revision of the same day, as a re-collection creates.
    with Manifest(paths.manifest_db) as man:
        man.record_batch(
            BatchRecord(
                batch_id=f"eod-{target.isoformat()}-r2",
                dataset="eod",
                status=manifest_module.PUBLISHED,
                source="test",
                params={},
                schema_version=schemas.SCHEMA_VERSION,
                ingested_at=manifest_module.utcnow(),
                partition_day=target,
                revision=2,
                rows=99,
                source_rows=rows_before,
                published_at=manifest_module.utcnow(),
            )
        )

    after = burnin_report(paths, required=10, window=10, verify_checksums=False)
    assert next(d.rows for d in after.days if d.day == target) == rows_before


def test_a_quarantined_revision_still_counts_as_a_fault(tmp_path):
    """A quarantine happened; a later good revision does not erase the record."""
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    days = CAL.recent_trading_days(date(2026, 9, 30), 10)
    populate(paths, days)
    with Manifest(paths.manifest_db) as man:
        man.record_batch(
            BatchRecord(
                batch_id="quarantined-r2",
                dataset="eod",
                status=manifest_module.QUARANTINED,
                source="test",
                params={},
                schema_version=schemas.SCHEMA_VERSION,
                ingested_at=manifest_module.utcnow(),
                partition_day=days[-1],
                revision=2,
                reason="high < low",
            )
        )
    report = burnin_report(paths, required=10, window=10, verify_checksums=False)
    assert not report.passed
    assert any("quarantined" in r for d in report.days for r in d.reasons)


def test_published_bytes_are_measured_from_disk_not_summed_from_batches(tmp_path):
    """Several EOD batches share one monthly file, so summing their sizes is wrong."""
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    populate(paths, CAL.recent_trading_days(date(2026, 9, 30), 10))

    on_disk = sum(p.stat().st_size for p in paths.published.rglob("*.parquet"))
    report = burnin_report(paths, required=10, window=10, verify_checksums=False)
    assert report.total_bytes == on_disk


def test_a_day_with_only_a_universe_snapshot_is_not_clean(tmp_path):
    """The burn-in is about the daily bar datasets, not any batch that happens to land."""
    paths = storage.DataPaths.from_root(tmp_path / "data").ensure()
    day = date(2026, 9, 30)
    with Manifest(paths.manifest_db) as man:
        man.expect("universe", [day])
        man.mark_coverage("universe", day, manifest_module.OK)
        man.record_batch(
            BatchRecord(
                batch_id="u1",
                dataset="universe",
                status=manifest_module.PUBLISHED,
                source="derived",
                params={},
                schema_version=schemas.SCHEMA_VERSION,
                ingested_at=manifest_module.utcnow(),
                partition_day=day,
                rows=10,
                source_rows=10,
                published_at=manifest_module.utcnow(),
            )
        )
    report = burnin_report(paths, required=1, window=1, verify_checksums=False)
    verdict = next(d for d in report.days if d.day == day)
    assert not verdict.clean
    assert "no daily dataset collected" in " ".join(verdict.reasons)
