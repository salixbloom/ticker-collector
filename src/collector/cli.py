"""Command line entry points.

``serve`` is what the container runs. The other subcommands exist so that an
operator (and the test suite) can inspect state without a running container.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta
from datetime import time as dtime
from pathlib import Path

from . import __version__, alerts, burnin, health, jobs, source, storage
from . import backup as backup_module
from . import manifest as manifest_module
from . import status as status_module
from .checks.trading_calendar import TradingCalendar
from .config import Config, ConfigError, load
from .logging_setup import setup as setup_logging
from .ratelimit import RateLimiter
from .scheduler import Scheduler

log = logging.getLogger("collector")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="collector", description="Market data collector")
    parser.add_argument("--config", help="path to collector.toml")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("serve", help="run the scheduler (the container entry point)")
    sub.add_parser("healthcheck", help="exit 0 if healthy, 1 otherwise")
    sub.add_parser("status", help="print status.json")
    sub.add_parser("init", help="create the data layout and manifest, then exit")

    verify = sub.add_parser("verify", help="re-hash published files against the manifest")
    verify.add_argument("--dataset", help="limit to one dataset")

    info = sub.add_parser("info", help="print resolved configuration (no secrets)")
    info.add_argument("--json", action="store_true", dest="as_json")

    eod = sub.add_parser("eod", help="fill missing EOD trading days")
    eod.add_argument("--days", type=int, help="backfill window in calendar days")
    eod.add_argument(
        "--max-days", type=int, help="stop after this many day(s); the rest resume next run"
    )
    eod.add_argument("--today", help="treat this date as today (ISO), for testing")

    doctor = sub.add_parser(
        "doctor", help="check configuration, paths, permissions and connectivity"
    )
    doctor.add_argument("--offline", action="store_true", help="skip the API connectivity check")

    reference = sub.add_parser("reference", help="snapshot all tickers (active and inactive)")
    reference.add_argument("--today", help="treat this date as today (ISO), for testing")

    corpactions = sub.add_parser("corporate-actions", help="fetch splits and dividends")
    corpactions.add_argument("--today", help="treat this date as today (ISO), for testing")

    universe = sub.add_parser("universe", help="rebuild the minute universe from EOD data")
    universe.add_argument("--size", type=int, help="number of tickers (default from config)")
    universe.add_argument("--today", help="treat this date as today (ISO), for testing")

    minute = sub.add_parser("minute", help="fill missing minute ticker-days")
    minute.add_argument("--days", type=int, help="window in calendar days")
    minute.add_argument(
        "--chunk-days",
        type=int,
        default=1,
        help="days per API call; >1 uses the backfill strategy (~10x fewer calls)",
    )
    minute.add_argument(
        "--max-calls", type=int, help="stop after this many calls; the rest resume next run"
    )
    minute.add_argument("--ticker", action="append", help="limit to these tickers (repeatable)")
    minute.add_argument("--today", help="treat this date as today (ISO), for testing")

    gaps = sub.add_parser("gaps", help="show what each dataset still owes")
    gaps.add_argument("--dataset", default="eod")

    backup = sub.add_parser("backup", help="write a verified backup")
    backup.add_argument("--to", dest="to", help="destination (default from config)")
    backup.add_argument(
        "--no-data",
        action="store_true",
        help="back up only the manifest and status, not published/",
    )
    backup.add_argument("--label", help="suffix for the backup directory name")
    backup.add_argument("--keep", type=int, help="prune to this many backups afterwards")

    backups = sub.add_parser("backups", help="list backups")
    backups.add_argument("--in", dest="in_dir", help="directory (default from config)")

    restore = sub.add_parser("restore", help="restore a backup into the data root")
    restore.add_argument("from_dir", help="the backup directory to restore")
    restore.add_argument("--force", action="store_true", help="overwrite existing published data")

    verify_backup = sub.add_parser("verify-backup", help="re-hash a backup's files")
    verify_backup.add_argument("backup_dir", help="the backup directory to check")

    burn = sub.add_parser("burnin", help="M10 burn-in status over recent trading days")
    burn.add_argument(
        "--days", type=int, help=f"clean days required (default {burnin.REQUIRED_CLEAN_DAYS})"
    )
    burn.add_argument("--window", type=int, help="trading days to examine")
    burn.add_argument("--today", help="treat this date as today (ISO), for testing")
    burn.add_argument("--json", action="store_true", dest="as_json")
    burn.add_argument("--journal", help="write the markdown write-up to this path")
    burn.add_argument(
        "--skip-checksums", action="store_true", help="skip re-hashing published files"
    )

    alert_test = sub.add_parser("alert-test", help="send a test alert to the configured webhook")
    alert_test.add_argument("--message", default="test alert from tick-collector")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load(args.config)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    # The health check runs as a separate short-lived process many times a day;
    # it must not write to the log file or it would dominate it.
    if args.command != "healthcheck":
        setup_logging(log_dir=config.data_root / "logs")

    handlers = {
        "serve": cmd_serve,
        "healthcheck": cmd_healthcheck,
        "status": cmd_status,
        "init": cmd_init,
        "doctor": cmd_doctor,
        "verify": cmd_verify,
        "info": cmd_info,
        "eod": cmd_eod,
        "reference": cmd_reference,
        "corporate-actions": cmd_corporate_actions,
        "universe": cmd_universe,
        "minute": cmd_minute,
        "gaps": cmd_gaps,
        "backup": cmd_backup,
        "backups": cmd_backups,
        "restore": cmd_restore,
        "verify-backup": cmd_verify_backup,
        "burnin": cmd_burnin,
        "alert-test": cmd_alert_test,
    }
    return handlers[args.command](config, args)


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #


def cmd_init(config: Config, _args: argparse.Namespace) -> int:
    paths = storage.DataPaths.from_root(config.data_root)
    paths.ensure()
    paths.check_writable()
    paths.check_same_filesystem()
    with manifest_module.Manifest(paths.manifest_db):
        pass  # opening runs the migrations
    cleared = storage.clear_staging(paths)
    log.info("initialised %s (cleared %d stale staging batch(es))", config.data_root, cleared)

    current = status_module.read(paths.status_file) or status_module.Status()
    current.started_at = status_module.utcnow().isoformat()
    current.disk = status_module.disk_usage(config.data_root)
    status_module.write(current, paths.status_file)
    return 0


def cmd_doctor(config: Config, args: argparse.Namespace) -> int:
    """Check everything a deployment can get wrong, and say what to do about it.

    Written for a container deployment where the usual debugging loop is slow:
    each line is a self-contained verdict, and a failure names the fix rather than
    leaving an errno to interpret.
    """
    paths = storage.DataPaths.from_root(config.data_root)
    results: list[tuple[bool, str]] = []

    def check(label: str, fn) -> None:
        try:
            detail = fn()
        except Exception as exc:  # a doctor must report faults, not raise them
            results.append((False, f"{label}: {exc}"))
        else:
            results.append((True, f"{label}: {detail}"))

    check("config", lambda: f"loaded from {config.config_path or '(defaults only)'}")
    check("timezone", lambda: f"{config.scheduler.timezone} -> {config.scheduler.tz}")
    check("data root", lambda: f"{config.data_root}")

    def writable() -> str:
        paths.ensure()
        paths.check_writable()
        return "published/ staging/ quarantine/ state/ logs/ all writable"

    check("permissions", writable)

    def atomic_publish() -> str:
        paths.check_same_filesystem()
        return "published/, staging/, quarantine/ share a filesystem"

    check("atomic publish", atomic_publish)

    def disk() -> str:
        usage = status_module.disk_usage(config.data_root)
        if not usage:
            return "unknown"
        free_gb = usage["free_bytes"] / 1e9
        if free_gb < 5:
            raise RuntimeError(f"only {free_gb:.1f} GB free on the data volume")
        return f"{free_gb:.1f} GB free"

    check("disk space", disk)

    def migrations() -> str:
        with manifest_module.Manifest(paths.manifest_db) as man:
            version = man.conn.execute("PRAGMA user_version").fetchone()[0]
            batches = man.conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"]
        return f"schema v{version}, {batches} batch(es) recorded"

    check("manifest", migrations)
    check(
        "api key",
        lambda: (
            "present in the environment"
            if config.has_api_key()
            else _fail("not set -- put MASSIVE_API_KEY in the stack's .env")
        ),
    )

    if not args.offline and config.has_api_key():

        def connectivity() -> str:
            ctx, man = _job_context(config)
            try:
                rows = ctx.client.splits(executed_after=date.today())
                return (
                    f"{config.source.base_url} reachable and authenticated "
                    f"(splits endpoint returned {len(rows)} row(s))"
                )
            finally:
                ctx.client.close()
                man.close()

        check("api reachable", connectivity)
    else:
        results.append((True, "api reachable: skipped"))

    published = list(paths.iter_published())
    results.append((True, f"published files: {len(published)}"))
    current = status_module.read(paths.status_file)
    if current is not None:
        healthy, reason = health.evaluate(config)
        results.append((healthy, f"health: {reason}"))

    width = max(len(line.partition(":")[0]) for _ok, line in results)
    for ok, line in results:
        label, _, detail = line.partition(":")
        verdict = "ok" if ok else "FAIL"
        print(f"[{verdict:>4}] {label:<{width}}  {detail.strip()}")

    failures = [line for ok, line in results if not ok]
    if failures:
        print(f"\n{len(failures)} problem(s) found.", file=sys.stderr)
        return 1
    print("\nAll checks passed.")
    return 0


def _fail(message: str):
    raise RuntimeError(message)


def cmd_serve(config: Config, args: argparse.Namespace) -> int:
    """Run the scheduler. This is what the container runs.

    One shared context, so one rate limiter and one HTTP client own the API key's
    whole call budget -- the plan's reason for a single long-running process.
    """
    log.info("collector %s starting", __version__)
    if cmd_init(config, args) != 0:
        return 1

    paths = storage.DataPaths.from_root(config.data_root)

    # One alerter for the process, so its dedup state is shared between the
    # heartbeat's health transitions and the jobs' failures.
    alerter = alerts.from_config(config, paths.state)
    alerter.cooldown = timedelta(hours=config.alerts.cooldown_hours)
    if not config.alert_webhook:
        log.info("no COLLECTOR_ALERT_WEBHOOK set; failures will be logged but not alerted")

    scheduler = Scheduler(
        timezone=config.scheduler.timezone,
        tick_seconds=min(config.scheduler.heartbeat_seconds, 60),
    )
    scheduler.install_signal_handlers()
    scheduler.set_heartbeat(lambda: _heartbeat(config, paths, alerter))

    if not config.has_api_key():
        # Not fatal: the container should come up, write a heartbeat and stay up so
        # the operator can see *why* it is idle rather than watch it crash-loop.
        log.warning(
            "no API key in the environment; collection jobs are NOT registered. "
            "Set MASSIVE_API_KEY (see .env.example) and restart."
        )
        scheduler.run_forever()
        _heartbeat(config, paths)
        log.info("collector stopped")
        return 0

    ctx, man = _job_context(config)
    try:
        _register_jobs(scheduler, ctx, alerter)
        if config.scheduler.catch_up_on_start:
            # A fresh deployment should show signs of life rather than sit idle
            # until 18:00. Every job is "fill what is missing", so running them on
            # startup is safe and idempotent -- a second run finds nothing to do.
            scheduler.run_once_on_start(delay_seconds=config.scheduler.catch_up_delay_seconds)
        scheduler.run_forever()
    finally:
        ctx.client.close()
        man.close()

    _heartbeat(config, paths)
    log.info("collector stopped")
    return 0


def _register_jobs(
    scheduler: Scheduler, ctx: jobs.Context, alerter: alerts.Alerter | None = None
) -> None:
    """Register the collection jobs, on the plan's schedule (US Eastern).

        reference          18:00                        (M5)
        corporate_actions  18:15                        (M5)
        eod                18:30, retry 07:00 next day  (M4)
        universe           19:00, monthly               (M6)
        minute             19:15, runs until done       (M7)
        checks_report      20:00                        (M2)

    Times only decide when catch-up happens: each job derives its work from the
    manifest, so a slot that is missed is simply picked up at the next one.
    Corporate actions run before EOD so that the day's splits are on the books
    before the large-move check looks for them.
    """

    notifier = alerter or alerts.Alerter()

    def scheduled(name: str, runner):
        def run() -> None:
            try:
                result = runner(ctx)
            except Exception as exc:
                # A job that raises -- a revoked API key, an unwritable volume --
                # must still reach status.json, or the container keeps reporting
                # healthy while collecting nothing until the 26h staleness window
                # finally catches it.
                log.exception("%s raised", name)
                detail = f"{type(exc).__name__}: {exc}"
                failed = jobs.JobResult(job=name, run_id="-", errors=[detail])
                jobs.write_status(ctx, [failed])
                alerts.report_job(notifier, name, "hard_failure", detail)
                return
            jobs.write_status(ctx, [result])
            if result.outcome not in ("ok", "soft_flags"):
                log.error("%s finished %s", name, result.outcome)
            alerts.report_job(
                notifier,
                name,
                result.outcome,
                "; ".join((result.errors + result.quarantine_reasons)[:3])
                or f"{result.job} {result.outcome}",
            )

        return run

    scheduler.every_day_at("reference", scheduled("reference", jobs.run_reference), dtime(18, 0))
    scheduler.every_day_at(
        "corporate_actions",
        scheduled("corporate_actions", jobs.run_corporate_actions),
        dtime(18, 15),
    )
    scheduler.every_day_at("eod", scheduled("eod", jobs.run_eod), dtime(18, 30), dtime(7, 0))
    # Monthly, and before the minute job, so a rebuilt universe takes effect the
    # same evening rather than a day later.
    scheduler.monthly_at("universe", scheduled("universe", jobs.run_universe), dtime(19, 0))
    scheduler.every_day_at("minute", scheduled("minute", _minute_runner(ctx)), dtime(19, 15))
    scheduler.every_day_at(
        "checks_report", scheduled("checks_report", jobs.run_checks_report), dtime(20, 0)
    )
    # After the day's collection, so a backup captures a complete day.
    scheduler.every_day_at("backup", scheduled("backup", jobs.run_backup), dtime(21, 0))


def _minute_runner(ctx: jobs.Context):
    """The scheduled minute job, bounded so it cannot run past the next day.

    The plan has this job "run until done", but a two-year backfill is ~5,000
    calls at 5/minute. Bounding each run keeps the scheduler responsive and the
    remainder is simply picked up tomorrow; nothing is lost because progress is
    recorded per ticker-day.
    """
    budget = ctx.config.collection.minute_calls_per_run

    def run(_ctx: jobs.Context) -> jobs.JobResult:
        return jobs.run_minute(_ctx, max_calls=budget)

    return run


def _heartbeat(
    config: Config, paths: storage.DataPaths, alerter: alerts.Alerter | None = None
) -> None:
    """Refresh status.json so the health check can see the process is alive.

    Also the place health transitions are noticed: the container's HEALTHCHECK
    runs in a separate short-lived process and so cannot remember whether it has
    already alerted, while this one can.
    """
    current = status_module.read(paths.status_file) or status_module.Status()
    current.disk = status_module.disk_usage(config.data_root)
    status_module.write(current, paths.status_file)
    if alerter is not None:
        healthy, reason = health.evaluate(config)
        alerts.report_health(alerter, healthy, reason)


def _job_context(config: Config) -> tuple[jobs.Context, manifest_module.Manifest]:
    """Assemble the shared job context: one limiter and one client per process."""
    paths = storage.DataPaths.from_root(config.data_root).ensure()
    paths.check_same_filesystem()
    man = manifest_module.Manifest(paths.manifest_db)
    client = source.Client(
        config=config,
        limiter=RateLimiter(config.source.calls_per_minute),
    )
    ctx = jobs.Context(
        config=config,
        client=client,
        manifest=man,
        paths=paths,
        calendar=TradingCalendar(),
    )
    return ctx, man


def cmd_eod(config: Config, args: argparse.Namespace) -> int:
    if not config.has_api_key():
        print("no API key; see .env.example", file=sys.stderr)
        return 2
    today = date.fromisoformat(args.today) if getattr(args, "today", None) else None
    ctx, man = _job_context(config)
    try:
        result = jobs.run_eod(ctx, today=today, backfill_days=args.days, max_days=args.max_days)
        jobs.write_status(ctx, [result])
        _report_result(config, ctx.paths, result)
    finally:
        ctx.client.close()
        man.close()

    print(
        f"eod: published={result.published} quarantined={result.quarantined} "
        f"errors={len(result.errors)} outcome={result.outcome}"
    )
    for flag in result.soft_flags[:10]:
        print(f"  soft: {flag}")
    for error in result.errors[:10]:
        print(f"  ERROR: {error}")
    return 0 if result.outcome in ("ok", "soft_flags") else 1


def _report_result(config: Config, paths: storage.DataPaths, result: jobs.JobResult) -> None:
    """Alert on a one-shot job's outcome, as the scheduled path does.

    A backfill or a repair run is often the thing most worth hearing about, and
    DEPLOY.md tells an operator to run those by hand -- so leaving them silent
    would mean the loudest failures were the ones nobody was told about.
    """
    alerter = alerts.from_config(config, paths.state)
    alerter.cooldown = timedelta(hours=config.alerts.cooldown_hours)
    alerts.report_job(
        alerter,
        result.job,
        result.outcome,
        "; ".join((result.errors + result.quarantine_reasons)[:3])
        or f"{result.job} {result.outcome}",
    )


def _print_result(result: jobs.JobResult) -> None:
    print(
        f"{result.job}: published={result.published} quarantined={result.quarantined} "
        f"errors={len(result.errors)} rows={result.rows} outcome={result.outcome}"
    )
    for flag in result.soft_flags[:10]:
        print(f"  soft: {flag}")
    for error in (result.errors + result.quarantine_reasons)[:10]:
        print(f"  ERROR: {error}")


def _run_job(config: Config, args: argparse.Namespace, runner) -> int:
    """Shared plumbing for the one-shot job commands."""
    if not config.has_api_key():
        print("no API key; see .env.example", file=sys.stderr)
        return 2
    today = date.fromisoformat(args.today) if getattr(args, "today", None) else None
    ctx, man = _job_context(config)
    try:
        result = runner(ctx, today)
        jobs.write_status(ctx, [result])
        _report_result(config, ctx.paths, result)
    finally:
        ctx.client.close()
        man.close()
    _print_result(result)
    return 0 if result.outcome in ("ok", "soft_flags") else 1


def cmd_reference(config: Config, args: argparse.Namespace) -> int:
    return _run_job(config, args, lambda ctx, today: jobs.run_reference(ctx, today=today))


def cmd_corporate_actions(config: Config, args: argparse.Namespace) -> int:
    return _run_job(config, args, lambda ctx, today: jobs.run_corporate_actions(ctx, today=today))


def cmd_universe(config: Config, args: argparse.Namespace) -> int:
    # Derived from published EOD data, so it needs no API key at all.
    today = date.fromisoformat(args.today) if getattr(args, "today", None) else None
    ctx, man = _job_context(config)
    try:
        result = jobs.run_universe(ctx, today=today, size=args.size)
        jobs.write_status(ctx, [result])
        _report_result(config, ctx.paths, result)
    finally:
        ctx.client.close()
        man.close()
    _print_result(result)
    return 0 if result.outcome in ("ok", "soft_flags") else 1


def cmd_minute(config: Config, args: argparse.Namespace) -> int:
    return _run_job(
        config,
        args,
        lambda ctx, today: jobs.run_minute(
            ctx,
            today=today,
            backfill_days=args.days,
            chunk_days=args.chunk_days,
            max_calls=args.max_calls,
            tickers=args.ticker,
        ),
    )


def _backup_dir(config: Config, given: str | None) -> Path:
    directory = given or config.backup.directory
    if not directory:
        raise ConfigError(
            "no backup directory: pass --to, or set [backup] directory in collector.toml"
        )
    return Path(directory)


def cmd_backup(config: Config, args: argparse.Namespace) -> int:
    paths = storage.DataPaths.from_root(config.data_root)
    try:
        destination = _backup_dir(config, args.to)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    try:
        info = backup_module.create(
            paths,
            destination,
            include_data=not args.no_data,
            label=args.label,
        )
        problems = backup_module.verify(info.path)
    except (backup_module.BackupError, OSError) as exc:
        print(f"backup failed: {exc}", file=sys.stderr)
        return 1

    if problems:
        print(f"backup wrote {info.path} but did NOT verify:", file=sys.stderr)
        for problem in problems[:10]:
            print(f"  {problem}", file=sys.stderr)
        return 1

    print(
        f"{info.path}: {info.files} file(s), {info.bytes / 1e6:,.1f} MB, "
        f"data={'yes' if info.includes_data else 'no'}, verified"
    )
    keep = args.keep if args.keep is not None else config.backup.keep
    removed = backup_module.prune(destination, keep)
    for path in removed:
        print(f"pruned {path.name}")
    return 0


def cmd_backups(config: Config, args: argparse.Namespace) -> int:
    try:
        destination = _backup_dir(config, args.in_dir)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    found = backup_module.list_backups(destination)
    if not found:
        print(f"no backups in {destination}")
        return 0
    print(f"{'backup':<34} {'files':>7} {'MB':>9}  data  schema")
    for info in found:
        flag = "" if backup_module.schema_matches_current(info) else "  (schema differs!)"
        print(
            f"{info.path.name:<34} {info.files:>7} {info.bytes / 1e6:>9,.1f}  "
            f"{'yes' if info.includes_data else ' no':<4}  v{info.schema_version}{flag}"
        )
    return 0


def cmd_verify_backup(_config: Config, args: argparse.Namespace) -> int:
    problems = backup_module.verify(Path(args.backup_dir))
    if problems:
        print(f"{args.backup_dir} did NOT verify:", file=sys.stderr)
        for problem in problems[:20]:
            print(f"  {problem}", file=sys.stderr)
        return 1
    print(f"{args.backup_dir}: every recorded file matches its checksum")
    return 0


def cmd_restore(config: Config, args: argparse.Namespace) -> int:
    paths = storage.DataPaths.from_root(config.data_root)
    try:
        info = backup_module.restore(Path(args.from_dir), paths, force=args.force)
    except (backup_module.BackupError, OSError) as exc:
        print(f"restore failed: {exc}", file=sys.stderr)
        return 1
    print(
        f"restored {info.files} file(s) from {info.path} "
        f"(taken {info.created_at}, collector {info.collector_version})"
    )
    if not backup_module.schema_matches_current(info):
        print(
            f"note: the backup's manifest schema is v{info.schema_version} and this build "
            f"expects v{manifest_module.SCHEMA_VERSION}; migrations run on next open.",
        )
    return 0


def cmd_burnin(config: Config, args: argparse.Namespace) -> int:
    paths = storage.DataPaths.from_root(config.data_root)
    today = date.fromisoformat(args.today) if getattr(args, "today", None) else None
    with manifest_module.Manifest(paths.manifest_db) as man:
        report = burnin.evaluate(
            man,
            paths,
            TradingCalendar(),
            today=today,
            required=args.days or burnin.REQUIRED_CLEAN_DAYS,
            window=args.window,
            verify_checksums=not args.skip_checksums,
        )

    if args.as_json:
        print(burnin.to_json(report))
    else:
        print(burnin.render(report))

    if args.journal:
        journal = Path(args.journal)
        journal.parent.mkdir(parents=True, exist_ok=True)
        journal.write_text(burnin.journal_entry(report, paths), encoding="utf-8")
        print(f"\nwrote {journal}")
    return 0 if report.passed else 1


def cmd_alert_test(config: Config, args: argparse.Namespace) -> int:
    paths = storage.DataPaths.from_root(config.data_root).ensure()
    alerter = alerts.from_config(config, paths.state)
    if not config.alert_webhook:
        print(
            "no COLLECTOR_ALERT_WEBHOOK set; nothing would be sent. "
            "Add it to the stack's .env to enable alerting.",
            file=sys.stderr,
        )
        return 2
    # A deliberate test bypasses the cooldown, which would otherwise swallow a
    # second attempt and look like a broken webhook.
    alerter.resolve("test")
    sent = alerter.notify("test", "test alert", args.message, severity="info")
    if not sent:
        print("the webhook did not accept the alert; see the log above", file=sys.stderr)
        return 1
    print("test alert delivered")
    alerter.resolve("test", "test alert cleared")
    return 0


def cmd_gaps(config: Config, args: argparse.Namespace) -> int:
    paths = storage.DataPaths.from_root(config.data_root)
    with manifest_module.Manifest(paths.manifest_db) as man:
        summary = man.coverage_summary(args.dataset)
        gaps = man.gaps(args.dataset)
    print(
        f"{args.dataset}: "
        + (", ".join(f"{k}={v}" for k, v in sorted(summary.items())) or "no coverage recorded")
    )
    for gap in gaps[:40]:
        label = f"  {gap.day.isoformat()}"
        if gap.ticker:
            label += f" {gap.ticker}"
        print(f"{label} {gap.state} attempts={gap.attempts} {gap.last_error or ''}".rstrip())
    if len(gaps) > 40:
        print(f"  ... and {len(gaps) - 40} more")
    return 0


def cmd_healthcheck(config: Config, _args: argparse.Namespace) -> int:
    healthy, reason = health.evaluate(config)
    print(reason)
    return 0 if healthy else 1


def cmd_status(config: Config, _args: argparse.Namespace) -> int:
    path = storage.DataPaths.from_root(config.data_root).status_file
    current = status_module.read(path)
    if current is None:
        print(f"no status file at {path}", file=sys.stderr)
        return 1
    print(current.to_json())
    return 0


def cmd_verify(config: Config, args: argparse.Namespace) -> int:
    paths = storage.DataPaths.from_root(config.data_root)
    with manifest_module.Manifest(paths.manifest_db) as man:
        problems = man.verify_checksums(config.data_root)
        batches = man.published_batches(getattr(args, "dataset", None))

    files = list(paths.iter_published(getattr(args, "dataset", None)))
    print(f"published files: {len(files)}; manifest published batches: {len(batches)}")
    if problems:
        for batch_id, why in problems:
            print(f"MISMATCH {batch_id}: {why}", file=sys.stderr)
        return 1
    print("all published files match their manifest checksums")
    return 0


def cmd_info(config: Config, args: argparse.Namespace) -> int:
    payload = {
        "version": __version__,
        "config_path": str(config.config_path) if config.config_path else None,
        "data_root": str(config.data_root),
        "api_key_present": config.has_api_key(),  # presence only, never the value
        "storage": vars(config.storage),
        "source": vars(config.source),
        "universe": vars(config.universe),
        "collection": vars(config.collection),
        "checks": vars(config.checks),
        "scheduler": vars(config.scheduler),
        "health": vars(config.health),
    }
    if getattr(args, "as_json", False):
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        for key, value in payload.items():
            print(f"{key}: {value}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
