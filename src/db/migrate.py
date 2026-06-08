"""Apply Alembic migrations to the data-service metrics SQLite database.

The metrics DB is an independent Alembic history (the ``[metrics]`` section in
``alembic.ini``); the connection URL is injected here because the path comes from
runtime config (``settings.metrics_db_path``). Used both by the app's startup
lifespan and the test fixtures, so migrations are applied exactly the same way
everywhere.
"""

import fcntl
import sqlite3
from pathlib import Path

from alembic import command
from alembic.config import Config as AlembicConfig

from settings import Settings

# alembic.ini lives at the repo root (this file is src/db/migrate.py).
_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


def _enable_wal(db_path: Path) -> None:
    """Persist WAL journal mode on the file (autocommit; never inside a txn)."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    finally:
        conn.close()


def _config_for(section: str, db_path: Path) -> AlembicConfig:
    """Build an Alembic config for one named DB section with the URL injected."""
    cfg = AlembicConfig(str(_ALEMBIC_INI))
    cfg.config_ini_section = section
    cfg.set_section_option(section, "sqlalchemy.url", f"sqlite:///{db_path.resolve()}")
    return cfg


def run_migrations(metrics_db_path: Path) -> None:
    """Upgrade the metrics database to ``head``, creating parent dirs as needed."""
    metrics_db_path.parent.mkdir(parents=True, exist_ok=True)
    command.upgrade(_config_for("metrics", metrics_db_path), "head")
    _enable_wal(metrics_db_path)


def ensure_migrations(settings: Settings) -> None:
    """Apply migrations at process startup, serialized across processes.

    A POSIX ``flock`` on a lockfile next to the DB guarantees only one process
    migrates at a time; the rest block briefly and then ``upgrade head`` no-ops
    at the stamped version. This is race-free because SQLite already pins every
    process to the same host and local volume, so a same-host advisory lock is
    exactly the right coordination primitive.
    """
    metrics_db_path = Path(settings.metrics_db_path)
    lock_path = metrics_db_path.parent / ".metrics_migrate.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)  # released when the fd closes
        run_migrations(metrics_db_path)
