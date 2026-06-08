"""Alembic environment for the metrics database (online migrations only).

The connection URL is injected by ``src/db/migrate.py`` into this config's
section. Migrations run with batch-mode rendering so SQLite ALTERs that need a
table rebuild work. WAL journal mode is set by the migrate helper (it must not be
toggled inside a transaction), so it is not configured here.
"""

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

# No ORM models — revisions are hand-authored, so autogenerate is unused.
target_metadata = None


def run_migrations_online() -> None:
    """Run migrations against a live connection."""
    section = config.get_section(config.config_ini_section, {})
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,
        )
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
