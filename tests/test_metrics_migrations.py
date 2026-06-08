"""Tests for the metrics-DB Alembic migration helpers (db.migrate)."""

import sqlite3
from types import SimpleNamespace

from db.migrate import ensure_migrations, run_migrations

_TABLES = {"sync_cycles", "redis_memory_samples", "redis_info_samples"}


def _tables(path) -> set[str]:
    conn = sqlite3.connect(str(path))
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return {row[0] for row in rows}
    finally:
        conn.close()


def _version(path) -> str | None:
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def test_run_migrations_creates_schema_at_head(tmp_path):
    metrics = tmp_path / "metrics.sqlite"
    run_migrations(metrics)
    assert _TABLES <= _tables(metrics)
    assert _version(metrics) == "metrics_0001"


def test_migrations_are_idempotent(tmp_path):
    metrics = tmp_path / "metrics.sqlite"
    run_migrations(metrics)
    run_migrations(metrics)  # must not raise
    assert _version(metrics) == "metrics_0001"


def test_adopts_existing_database_without_losing_data(tmp_path):
    """An existing DB (table present, no alembic_version) is adopted, not recreated."""
    metrics = tmp_path / "metrics.sqlite"
    conn = sqlite3.connect(str(metrics))
    conn.execute(
        "CREATE TABLE sync_cycles (id INTEGER PRIMARY KEY, domain TEXT, "
        "started_at TEXT, finished_at TEXT, duration_ms INTEGER, "
        "downloaded INTEGER, errors INTEGER, outcome TEXT)"
    )
    conn.execute(
        "INSERT INTO sync_cycles "
        "(domain, started_at, finished_at, duration_ms, downloaded, errors, outcome) "
        "VALUES ('satellite', 't', 't', 1, 1, 0, 'ok')"
    )
    conn.commit()
    conn.close()

    run_migrations(metrics)

    assert _version(metrics) == "metrics_0001"
    conn = sqlite3.connect(str(metrics))
    try:
        assert (
            conn.execute("SELECT domain FROM sync_cycles").fetchone()[0] == "satellite"
        )
    finally:
        conn.close()


def test_ensure_migrations_applies_under_lock(tmp_path):
    """The startup entry migrates the DB and creates the coordination lockfile."""
    settings = SimpleNamespace(metrics_db_path=str(tmp_path / "metrics.sqlite"))

    ensure_migrations(settings)
    ensure_migrations(settings)  # idempotent: a second call is a no-op

    assert _TABLES <= _tables(tmp_path / "metrics.sqlite")
    assert _version(tmp_path / "metrics.sqlite") == "metrics_0001"
    assert (tmp_path / ".metrics_migrate.lock").exists()
