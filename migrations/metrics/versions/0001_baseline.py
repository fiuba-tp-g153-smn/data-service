"""baseline: metrics schema

Revision ID: metrics_0001
Revises:
Create Date: 2026-06-08

Idempotent baseline mirroring the schema that ``MetricsStore._open_and_init``
used to create inline. It creates the three tables + their indexes only when the
tables are absent, so ``alembic upgrade head`` adopts a pre-existing database
(one created by the old ``CREATE TABLE IF NOT EXISTS`` path, with no
``alembic_version`` table) by simply stamping this revision rather than failing
on a duplicate table.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "metrics_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    if sa.inspect(bind).has_table("sync_cycles"):
        return  # adopt an existing database: just stamp this revision

    op.create_table(
        "sync_cycles",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("domain", sa.Text, nullable=False),
        sa.Column("started_at", sa.Text, nullable=False),
        sa.Column("finished_at", sa.Text, nullable=False),
        sa.Column("duration_ms", sa.Integer, nullable=False),
        sa.Column("downloaded", sa.Integer, nullable=False, server_default="0"),
        sa.Column("errors", sa.Integer, nullable=False, server_default="0"),
        sa.Column("outcome", sa.Text, nullable=False),
        sqlite_autoincrement=True,
    )
    op.create_index(
        "idx_sync_cycles_domain_finished", "sync_cycles", ["domain", "finished_at"]
    )
    op.create_index("idx_sync_cycles_finished", "sync_cycles", ["finished_at"])

    op.create_table(
        "redis_memory_samples",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("sampled_at", sa.Text, nullable=False),
        sa.Column("domain", sa.Text, nullable=False),
        sa.Column("key_count", sa.Integer, nullable=False),
        sa.Column("memory_bytes", sa.Integer, nullable=False),
        sqlite_autoincrement=True,
    )
    op.create_index("idx_memory_sampled", "redis_memory_samples", ["sampled_at"])
    op.create_index(
        "idx_memory_domain_sampled", "redis_memory_samples", ["domain", "sampled_at"]
    )

    op.create_table(
        "redis_info_samples",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("sampled_at", sa.Text, nullable=False),
        sa.Column("used_memory", sa.Integer),
        sa.Column("used_memory_rss", sa.Integer),
        sa.Column("used_memory_peak", sa.Integer),
        sa.Column("maxmemory", sa.Integer),
        sa.Column("mem_fragmentation_ratio", sa.Float),
        sa.Column("evicted_keys", sa.Integer),
        sa.Column("expired_keys", sa.Integer),
        sa.Column("keyspace_hits", sa.Integer),
        sa.Column("keyspace_misses", sa.Integer),
        sa.Column("connected_clients", sa.Integer),
        sa.Column("total_keys", sa.Integer),
        sqlite_autoincrement=True,
    )
    op.create_index("idx_info_sampled", "redis_info_samples", ["sampled_at"])


def downgrade() -> None:
    op.drop_table("redis_info_samples")
    op.drop_table("redis_memory_samples")
    op.drop_table("sync_cycles")
