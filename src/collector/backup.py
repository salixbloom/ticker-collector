"""Backup and restore.

The published data cannot be re-downloaded. The provider serves two years of
history, so anything that ages out of that window is gone for good -- which makes
the archive progressively irreplaceable and the backup the only real safety net.

Two things need care, and they are why this is a module rather than a `cp -r`:

* **The manifest is a live SQLite database in WAL mode.** Copying the file while
  the collector is running can capture a torn page or miss committed data sitting
  in the write-ahead log. SQLite's own backup API takes a consistent snapshot of a
  database being written to, so that is what is used.
* **A backup you have never restored is a guess.** Every backup records the
  SHA-256 of each file it contains, and :func:`verify` re-hashes them, so a
  restore can be confirmed rather than hoped for.
"""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from . import __version__
from . import manifest as manifest_module
from .storage import DataPaths, sha256_file

log = logging.getLogger(__name__)

#: Written into every backup directory.
MANIFEST_NAME = "backup.json"

#: Filename of the consistent manifest snapshot inside a backup.
DB_NAME = "manifest.sqlite"


class BackupError(RuntimeError):
    """Raised when a backup or restore cannot be completed or verified."""


@dataclass
class BackupInfo:
    """What a backup contains."""

    path: Path
    created_at: str
    files: int
    bytes: int
    includes_data: bool
    collector_version: str
    schema_version: int

    def to_dict(self) -> dict:
        return {
            "created_at": self.created_at,
            "collector_version": self.collector_version,
            "manifest_schema_version": self.schema_version,
            "files": self.files,
            "bytes": self.bytes,
            "includes_data": self.includes_data,
        }


def utcnow() -> datetime:
    from datetime import UTC

    return datetime.now(UTC)


def create(
    paths: DataPaths,
    destination: Path,
    *,
    include_data: bool = True,
    label: str | None = None,
) -> BackupInfo:
    """Write a verifiable backup into a timestamped directory under ``destination``.

    ``include_data=False`` backs up only the state -- the manifest and status --
    which is the cheap daily case when something else (an Unraid backup plugin,
    an offsite sync) already has ``published/``. The manifest is the part that
    cannot be reconstructed from the Parquet files, because it holds the
    checksums, the point-in-time ``ingested_at`` and the check results.
    """
    stamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
    name = f"backup-{stamp}" + (f"-{label}" if label else "")
    target = destination / name
    if target.exists():
        raise BackupError(f"{target} already exists")
    target.mkdir(parents=True)

    checksums: dict[str, str] = {}
    total = 0

    # The manifest first, via SQLite's backup API: a plain copy of a live WAL
    # database can be torn or stale.
    db_target = target / DB_NAME
    schema_version = _snapshot_database(paths.manifest_db, db_target)
    checksums[DB_NAME] = sha256_file(db_target)
    total += db_target.stat().st_size

    if paths.status_file.is_file():
        shutil.copy2(paths.status_file, target / "status.json")
        checksums["status.json"] = sha256_file(target / "status.json")
        total += (target / "status.json").stat().st_size

    if include_data:
        for source in sorted(paths.published.rglob("*.parquet")):
            relative = source.relative_to(paths.root)
            copy_to = target / relative
            copy_to.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, copy_to)
            checksums[relative.as_posix()] = sha256_file(copy_to)
            total += copy_to.stat().st_size

    info = BackupInfo(
        path=target,
        created_at=utcnow().isoformat(),
        files=len(checksums),
        bytes=total,
        includes_data=include_data,
        collector_version=__version__,
        schema_version=schema_version,
    )
    (target / MANIFEST_NAME).write_text(
        json.dumps({**info.to_dict(), "checksums": checksums}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    log.info(
        "backup: %s (%d file(s), %.1f MB, data=%s)",
        target,
        info.files,
        info.bytes / 1e6,
        include_data,
    )
    return info


def _snapshot_database(source: Path, target: Path) -> int:
    """Consistent copy of a live SQLite database, returning its schema version."""
    if not source.is_file():
        raise BackupError(f"no manifest database at {source}")
    src = sqlite3.connect(source)
    try:
        version = src.execute("PRAGMA user_version").fetchone()[0]
        dst = sqlite3.connect(target)
        try:
            # Walks the database under a read lock, so concurrent writes are
            # either fully included or fully excluded -- never half-applied.
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    return int(version)


def verify(backup_dir: Path) -> list[str]:
    """Re-hash every file in a backup. Returns a list of problems, empty if sound."""
    manifest_path = backup_dir / MANIFEST_NAME
    if not manifest_path.is_file():
        return [f"{backup_dir} has no {MANIFEST_NAME}; it is not a backup directory"]
    try:
        recorded = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"unreadable {MANIFEST_NAME}: {exc}"]

    problems: list[str] = []
    checksums = recorded.get("checksums", {})
    if not checksums:
        problems.append("backup records no checksums")
    for relative, expected in sorted(checksums.items()):
        path = backup_dir / relative
        if not path.is_file():
            problems.append(f"missing {relative}")
            continue
        actual = sha256_file(path)
        if actual != expected:
            problems.append(f"{relative}: {actual} != recorded {expected}")

    # A file present but unrecorded means the backup is not what it claims.
    present = {
        p.relative_to(backup_dir).as_posix()
        for p in backup_dir.rglob("*")
        if p.is_file() and p.name != MANIFEST_NAME
    }
    for extra in sorted(present - set(checksums)):
        problems.append(f"{extra}: present but not recorded in {MANIFEST_NAME}")
    return problems


def restore(backup_dir: Path, paths: DataPaths, *, force: bool = False) -> BackupInfo:
    """Restore a backup into ``paths``, verifying it first and after.

    Refuses to write over existing published data unless ``force`` is set, so a
    mistyped command cannot destroy the archive it was meant to protect.
    """
    problems = verify(backup_dir)
    if problems:
        raise BackupError(f"refusing to restore from {backup_dir}: " + "; ".join(problems[:5]))

    recorded = json.loads((backup_dir / MANIFEST_NAME).read_text(encoding="utf-8"))
    existing = list(paths.published.rglob("*.parquet")) if paths.published.exists() else []
    if existing and not force:
        raise BackupError(
            f"{paths.published} already holds {len(existing)} published file(s). "
            "Restoring would overwrite them; pass force to proceed."
        )

    paths.ensure()
    for relative in sorted(recorded.get("checksums", {})):
        source = backup_dir / relative
        if relative == DB_NAME:
            target = paths.manifest_db
        elif relative == "status.json":
            target = paths.status_file
        else:
            target = paths.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    # A restore that is not checked is a restore that might not have worked.
    restored_problems = []
    for relative, expected in sorted(recorded.get("checksums", {}).items()):
        if relative == DB_NAME:
            target = paths.manifest_db
        elif relative == "status.json":
            target = paths.status_file
        else:
            target = paths.root / relative
        if not target.is_file():
            restored_problems.append(f"missing after restore: {relative}")
        elif sha256_file(target) != expected:
            restored_problems.append(f"checksum mismatch after restore: {relative}")
    if restored_problems:
        raise BackupError("restore did not verify: " + "; ".join(restored_problems[:5]))

    # WAL files from the previous database would contradict the restored one.
    for suffix in ("-wal", "-shm"):
        stale = paths.manifest_db.with_name(paths.manifest_db.name + suffix)
        stale.unlink(missing_ok=True)

    info = BackupInfo(
        path=backup_dir,
        created_at=recorded.get("created_at", ""),
        files=len(recorded.get("checksums", {})),
        bytes=recorded.get("bytes", 0),
        includes_data=recorded.get("includes_data", False),
        collector_version=recorded.get("collector_version", "?"),
        schema_version=int(recorded.get("manifest_schema_version", 0)),
    )
    log.info("restored %d file(s) from %s", info.files, backup_dir)
    return info


def list_backups(destination: Path) -> list[BackupInfo]:
    """Every backup under ``destination``, newest first."""
    found: list[BackupInfo] = []
    if not destination.exists():
        return found
    for candidate in sorted(destination.iterdir(), reverse=True):
        manifest_path = candidate / MANIFEST_NAME
        if not manifest_path.is_file():
            continue
        try:
            recorded = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        found.append(
            BackupInfo(
                path=candidate,
                created_at=recorded.get("created_at", ""),
                files=len(recorded.get("checksums", {})),
                bytes=recorded.get("bytes", 0),
                includes_data=recorded.get("includes_data", False),
                collector_version=recorded.get("collector_version", "?"),
                schema_version=int(recorded.get("manifest_schema_version", 0)),
            )
        )
    return found


def prune(destination: Path, keep: int) -> list[Path]:
    """Delete all but the newest ``keep`` backups. Returns what was removed."""
    if keep < 1:
        raise BackupError("keep must be at least 1")
    backups = list_backups(destination)
    removed: list[Path] = []
    for info in backups[keep:]:
        shutil.rmtree(info.path, ignore_errors=True)
        removed.append(info.path)
    if removed:
        log.info("pruned %d old backup(s), keeping %d", len(removed), keep)
    return removed


def schema_matches_current(info: BackupInfo) -> bool:
    """Whether a backup's manifest schema matches what this build expects."""
    return info.schema_version == manifest_module.SCHEMA_VERSION
