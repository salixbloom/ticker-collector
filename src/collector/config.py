"""Configuration: a TOML file for settings, the environment for secrets.

The split is deliberate. The config file is mounted, readable and safe to put in
version control; the API key only ever arrives through the environment, so it
cannot be baked into the image or committed by accident.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

DEFAULT_CONFIG_PATH = Path("/app/config/collector.toml")

#: Environment variables holding secrets. Their values are redacted from logs.
SECRET_ENV_VARS = ("MASSIVE_API_KEY", "POLYGON_API_KEY", "COLLECTOR_ALERT_WEBHOOK")


class ConfigError(RuntimeError):
    """Raised when configuration is missing or invalid."""


@dataclass(frozen=True)
class StorageConfig:
    compression_level: int = 9
    row_group_bytes: int = 134_217_728


@dataclass(frozen=True)
class SourceConfig:
    base_url: str = "https://api.massive.com"
    calls_per_minute: int = 5
    max_retries: int = 5
    timeout_seconds: float = 30.0


@dataclass(frozen=True)
class UniverseConfig:
    size: int = 1000
    grace_days: int = 30
    #: Instrument types to include, from the reference snapshot's `type` column
    #: (CS = common stock, ETF, ADRC, PFD, FUND, ...). Empty means no filter,
    #: which is what the raw dollar-volume ranking gives: SPY and QQQ outrank
    #: every actual stock. Set ["CS"] for common stock only.
    types: tuple[str, ...] = ()


@dataclass(frozen=True)
class CollectionConfig:
    extended_hours: bool = True
    backfill_days: int = 730
    #: API calls the scheduled minute job may spend per run. At 5 calls/minute a
    #: 1,000-ticker day costs ~200 minutes, so an unbounded run would still be
    #: going when the next slot came round. The remainder resumes tomorrow.
    minute_calls_per_run: int = 1200
    #: Days per call when backfilling minute bars. One call returns up to 50,000
    #: bars and a dense extended-hours day is ~960, so 40 days leaves headroom.
    minute_backfill_chunk_days: int = 40


@dataclass(frozen=True)
class ChecksConfig:
    eod_count_soft_tolerance: float = 0.10
    eod_count_hard_tolerance: float = 0.30
    run_failure_abort_ratio: float = 0.20


@dataclass(frozen=True)
class SchedulerConfig:
    timezone: str = "America/New_York"
    heartbeat_seconds: int = 60
    #: Run every job once shortly after startup, rather than waiting for its slot.
    #: Safe because every job fills only what the manifest says is missing.
    catch_up_on_start: bool = True
    #: Grace period before that pass, so the health check comes up clean first.
    catch_up_delay_seconds: int = 120

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)


@dataclass(frozen=True)
class HealthConfig:
    max_status_age_seconds: int = 93_600  # 26 hours


@dataclass(frozen=True)
class AlertsConfig:
    #: Hours before a still-active alert is re-sent. Without a cooldown a
    #: provider outage would send a message every heartbeat all night.
    cooldown_hours: int = 6


@dataclass(frozen=True)
class BackupConfig:
    #: Where backups go. Empty disables the scheduled backup job; `collector
    #: backup --to DIR` still works by hand.
    directory: str = ""
    #: Include published/ in the scheduled backup. Turn this off when something
    #: else already copies the array (an Unraid backup plugin, an offsite sync)
    #: and only the manifest needs a consistent snapshot.
    include_data: bool = True
    #: Backups to keep before pruning the oldest.
    keep: int = 7


@dataclass(frozen=True)
class Config:
    data_root: Path
    storage: StorageConfig = field(default_factory=StorageConfig)
    source: SourceConfig = field(default_factory=SourceConfig)
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    collection: CollectionConfig = field(default_factory=CollectionConfig)
    checks: ChecksConfig = field(default_factory=ChecksConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    health: HealthConfig = field(default_factory=HealthConfig)
    alerts: AlertsConfig = field(default_factory=AlertsConfig)
    backup: BackupConfig = field(default_factory=BackupConfig)
    config_path: Path | None = None

    # -- secrets ------------------------------------------------------------- #

    @property
    def api_key(self) -> str:
        """The data provider API key, from the environment only.

        Never stored on the config object: keeping it behind a property means a
        ``repr`` of the config, or a config dump into a log or status file, cannot
        leak it.
        """
        for name in ("MASSIVE_API_KEY", "POLYGON_API_KEY"):
            value = os.environ.get(name, "").strip()
            if value:
                return value
        raise ConfigError(
            "no API key: set MASSIVE_API_KEY in the environment file (see .env.example)"
        )

    def has_api_key(self) -> bool:
        try:
            return bool(self.api_key)
        except ConfigError:
            return False

    @property
    def alert_webhook(self) -> str | None:
        return os.environ.get("COLLECTOR_ALERT_WEBHOOK", "").strip() or None


def load(path: str | os.PathLike[str] | None = None) -> Config:
    """Load configuration from TOML, falling back to the documented defaults.

    Resolution order: explicit argument, ``COLLECTOR_CONFIG``, then
    ``/app/config/collector.toml``. A missing file is not an error -- every
    setting has a default -- but a malformed one is.
    """
    candidate = path or os.environ.get("COLLECTOR_CONFIG") or DEFAULT_CONFIG_PATH
    config_path = Path(candidate)

    raw: dict[str, Any] = {}
    if config_path.is_file():
        try:
            raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"{config_path}: invalid TOML: {exc}") from exc
    elif path is not None:
        raise ConfigError(f"config file not found: {config_path}")

    data_root = Path(
        os.environ.get("COLLECTOR_DATA_ROOT") or raw.get("paths", {}).get("data_root") or "/data"
    )

    universe_raw = dict(raw.get("universe", {}))
    if "types" in universe_raw:
        universe_raw["types"] = tuple(universe_raw["types"])

    config = Config(
        data_root=data_root,
        storage=_build(StorageConfig, raw.get("storage", {}), "storage"),
        source=_build(SourceConfig, raw.get("source", {}), "source"),
        universe=_build(UniverseConfig, universe_raw, "universe"),
        collection=_build(CollectionConfig, raw.get("collection", {}), "collection"),
        checks=_build(ChecksConfig, raw.get("checks", {}), "checks"),
        scheduler=_build(SchedulerConfig, raw.get("scheduler", {}), "scheduler"),
        health=_build(HealthConfig, raw.get("health", {}), "health"),
        alerts=_build(AlertsConfig, raw.get("alerts", {}), "alerts"),
        backup=_build(BackupConfig, raw.get("backup", {}), "backup"),
        config_path=config_path if config_path.is_file() else None,
    )
    validate(config)
    return config


def _build(cls: type, values: dict[str, Any], section: str):
    """Instantiate a settings dataclass, rejecting unknown keys.

    Silently ignoring a typo'd key would mean running with a default the operator
    thought they had overridden, which is worse than failing at startup.
    """
    known = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
    unknown = set(values) - known
    if unknown:
        raise ConfigError(f"[{section}]: unknown key(s) {sorted(unknown)}; known: {sorted(known)}")
    return cls(**values)


def validate(config: Config) -> None:
    """Reject settings that would quietly break a guarantee in the plan."""
    if not 1 <= config.storage.compression_level <= 22:
        raise ConfigError("[storage] compression_level must be between 1 and 22 (zstd)")
    if config.source.calls_per_minute < 1:
        raise ConfigError("[source] calls_per_minute must be at least 1")
    if config.universe.size < 1:
        raise ConfigError("[universe] size must be at least 1")
    if config.universe.grace_days < 0:
        raise ConfigError("[universe] grace_days must not be negative")
    if config.collection.backfill_days < 1:
        raise ConfigError("[collection] backfill_days must be at least 1")
    if config.collection.minute_calls_per_run < 1:
        raise ConfigError("[collection] minute_calls_per_run must be at least 1")
    if not 1 <= config.collection.minute_backfill_chunk_days <= 120:
        raise ConfigError("[collection] minute_backfill_chunk_days must be between 1 and 120")
    for name, value in (
        ("eod_count_soft_tolerance", config.checks.eod_count_soft_tolerance),
        ("eod_count_hard_tolerance", config.checks.eod_count_hard_tolerance),
        ("run_failure_abort_ratio", config.checks.run_failure_abort_ratio),
    ):
        if not 0 < value <= 1:
            raise ConfigError(f"[checks] {name} must be in (0, 1]")
    if config.checks.eod_count_soft_tolerance > config.checks.eod_count_hard_tolerance:
        raise ConfigError("[checks] soft tolerance must not exceed the hard tolerance")
    if config.alerts.cooldown_hours < 0:
        raise ConfigError("[alerts] cooldown_hours must not be negative")
    if config.backup.keep < 1:
        raise ConfigError("[backup] keep must be at least 1")
    if config.scheduler.heartbeat_seconds < 5:
        raise ConfigError("[scheduler] heartbeat_seconds must be at least 5")
    if config.scheduler.catch_up_delay_seconds < 0:
        raise ConfigError("[scheduler] catch_up_delay_seconds must not be negative")
    try:
        _ = config.scheduler.tz
    except Exception as exc:
        raise ConfigError(f"[scheduler] unknown timezone {config.scheduler.timezone!r}") from exc


def secret_values() -> list[str]:
    """Current secret values, for the log redaction filter."""
    values = []
    for name in SECRET_ENV_VARS:
        value = os.environ.get(name, "").strip()
        if len(value) >= 4:  # too short to redact usefully, and not a real key
            values.append(value)
    return values
