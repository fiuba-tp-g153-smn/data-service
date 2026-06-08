"""Unit tests for `MetricsStore` (SQLite time-series for the status dashboard)."""

import sqlite3

import pytest
import pytest_asyncio

from clients.metrics_store import (
    InfoSample,
    MetricsStore,
    SyncCycleRow,
)

# Sortable ISO8601 UTC timestamps (isoformat-style with +00:00 offset).
T0900 = "2026-06-07T09:00:00+00:00"
T1000 = "2026-06-07T10:00:00+00:00"
T1030 = "2026-06-07T10:30:00+00:00"
T1100 = "2026-06-07T11:00:00+00:00"
OLD = "2026-05-01T00:00:00+00:00"


@pytest_asyncio.fixture
async def store(tmp_path):
    """Fresh store backed by a tmp_path sqlite file; closed after each test."""
    s = MetricsStore(str(tmp_path / "metrics.sqlite"))
    await s.connect()
    try:
        yield s
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_connect_creates_parent_directory(tmp_path):
    db_path = tmp_path / "nested" / "dir" / "metrics.sqlite"
    s = MetricsStore(str(db_path))
    await s.connect()
    try:
        assert db_path.exists()
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_operations_without_connect_raise(tmp_path):
    s = MetricsStore(str(tmp_path / "metrics.sqlite"))
    with pytest.raises(RuntimeError):
        await s.get_latest_sync_per_domain()


@pytest.mark.asyncio
async def test_wal_mode_enabled(tmp_path):
    db_path = tmp_path / "metrics.sqlite"
    s = MetricsStore(str(db_path))
    await s.connect()
    try:
        con = sqlite3.connect(str(db_path))
        mode = con.execute("PRAGMA journal_mode;").fetchone()[0]
        con.close()
        assert mode.lower() == "wal"
    finally:
        await s.close()


# --------------------------------------------------------------------------- #
# sync_cycles
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_latest_sync_per_domain_returns_newest_row(store):
    await store.record_sync_cycle("satellite", T0900, T0900, 100, 5, 0, "ok")
    await store.record_sync_cycle("satellite", T1000, T1000, 200, 9, 1, "error")
    await store.record_sync_cycle("radar", T1000, T1000, 50, 3, 0, "ok")

    latest = {row.domain: row for row in await store.get_latest_sync_per_domain()}
    assert set(latest) == {"satellite", "radar"}
    # The satellite row is the second (newest) insert.
    assert latest["satellite"] == SyncCycleRow(
        domain="satellite",
        started_at=T1000,
        finished_at=T1000,
        duration_ms=200,
        downloaded=9,
        errors=1,
        outcome="error",
    )


@pytest.mark.asyncio
async def test_get_sync_cycles_filters_since_domain_and_orders_desc(store):
    await store.record_sync_cycle("satellite", OLD, OLD, 1, 1, 0, "ok")  # filtered out
    await store.record_sync_cycle("satellite", T0900, T0900, 1, 1, 0, "ok")
    await store.record_sync_cycle("satellite", T1100, T1100, 1, 2, 0, "ok")
    await store.record_sync_cycle("radar", T1000, T1000, 1, 1, 0, "ok")

    rows = await store.get_sync_cycles(T0900, domain="satellite", limit=10)
    assert [r.finished_at for r in rows] == [T1100, T0900]  # newest first, no OLD

    limited = await store.get_sync_cycles(T0900, domain="satellite", limit=1)
    assert [r.finished_at for r in limited] == [T1100]


@pytest.mark.asyncio
async def test_get_sync_cycles_before_window_and_unlimited(store):
    await store.record_sync_cycle("satellite", T0900, T0900, 1, 1, 0, "ok")
    await store.record_sync_cycle("satellite", T1000, T1000, 1, 1, 0, "ok")
    await store.record_sync_cycle("satellite", T1100, T1100, 1, 1, 0, "ok")

    # before excludes rows with finished_at >= T1100; limit=0 returns all in window.
    rows = await store.get_sync_cycles(
        "2000-01-01T00:00:00+00:00", before_iso=T1100, limit=0
    )
    assert [r.finished_at for r in rows] == [T1000, T0900]


@pytest.mark.asyncio
async def test_sync_history_hourly_buckets_and_sums(store):
    await store.record_sync_cycle("satellite", T1000, T1000, 100, 5, 1, "error")
    await store.record_sync_cycle("satellite", T1030, T1030, 300, 7, 0, "ok")
    await store.record_sync_cycle("satellite", T1100, T1100, 200, 2, 0, "ok")

    buckets = await store.get_sync_history(T0900, bucket="hour", domain="satellite")
    by_bucket = {b.bucket: b for b in buckets}
    assert set(by_bucket) == {"2026-06-07T10", "2026-06-07T11"}

    ten = by_bucket["2026-06-07T10"]
    assert ten.cycles == 2
    assert ten.downloaded == 12  # 5 + 7
    assert ten.errors == 1
    assert ten.avg_duration_ms == 200.0  # (100 + 300) / 2


@pytest.mark.asyncio
async def test_sync_history_daily_bucket(store):
    await store.record_sync_cycle("radar", T1000, T1000, 100, 5, 0, "ok")
    await store.record_sync_cycle("radar", T1100, T1100, 100, 3, 0, "ok")

    buckets = await store.get_sync_history(T0900, bucket="day")
    assert len(buckets) == 1
    assert buckets[0].bucket == "2026-06-07"
    assert buckets[0].downloaded == 8


# --------------------------------------------------------------------------- #
# redis_memory_samples
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_latest_memory_returns_newest_sample_ordered_by_bytes(store):
    await store.record_memory_sample(T1000, [("satellite", 10, 100), ("radar", 5, 50)])
    await store.record_memory_sample(
        T1100, [("satellite", 12, 400), ("basemap", 99, 900)]
    )

    sampled_at, rows = await store.get_latest_memory()
    assert sampled_at == T1100
    # Ordered by memory_bytes DESC.
    assert [(r.domain, r.memory_bytes) for r in rows] == [
        ("basemap", 900),
        ("satellite", 400),
    ]


@pytest.mark.asyncio
async def test_latest_memory_empty_when_no_samples(store):
    sampled_at, rows = await store.get_latest_memory()
    assert sampled_at is None
    assert rows == []


@pytest.mark.asyncio
async def test_memory_history_filters_since_and_domain(store):
    await store.record_memory_sample(OLD, [("satellite", 1, 10)])  # filtered out
    await store.record_memory_sample(T1000, [("satellite", 2, 20), ("radar", 1, 5)])
    await store.record_memory_sample(T1100, [("satellite", 3, 30)])

    points = await store.get_memory_history(T0900, domain="satellite")
    assert [(p.sampled_at, p.memory_bytes) for p in points] == [
        (T1000, 20),
        (T1100, 30),
    ]


@pytest.mark.asyncio
async def test_record_memory_sample_empty_is_noop(store):
    await store.record_memory_sample(T1000, [])
    sampled_at, rows = await store.get_latest_memory()
    assert sampled_at is None
    assert rows == []


# --------------------------------------------------------------------------- #
# redis_info_samples
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_info_sample_round_trip_with_missing_fields(store):
    await store.record_info_sample(
        T1000,
        {
            "used_memory": 12345,
            "used_memory_rss": 20000,
            "mem_fragmentation_ratio": 1.25,
            "total_keys": 7,
            # other fields intentionally absent -> stored NULL
        },
    )
    latest = await store.get_latest_info()
    assert isinstance(latest, InfoSample)
    assert latest.used_memory == 12345
    assert latest.used_memory_rss == 20000
    assert latest.mem_fragmentation_ratio == 1.25
    assert latest.total_keys == 7
    assert latest.maxmemory is None
    assert latest.evicted_keys is None


@pytest.mark.asyncio
async def test_get_latest_info_none_when_empty(store):
    assert await store.get_latest_info() is None


@pytest.mark.asyncio
async def test_info_history_orders_by_sampled_at(store):
    await store.record_info_sample(T1100, {"used_memory": 200})
    await store.record_info_sample(T1000, {"used_memory": 100})
    history = await store.get_info_history(T0900)
    assert [h.used_memory for h in history] == [100, 200]


# --------------------------------------------------------------------------- #
# retention
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_prune_deletes_old_rows_across_tables(store):
    await store.record_sync_cycle("satellite", OLD, OLD, 1, 1, 0, "ok")
    await store.record_sync_cycle("satellite", T1100, T1100, 1, 1, 0, "ok")
    await store.record_memory_sample(OLD, [("satellite", 1, 10)])
    await store.record_memory_sample(T1100, [("satellite", 2, 20)])
    await store.record_info_sample(OLD, {"used_memory": 1})
    await store.record_info_sample(T1100, {"used_memory": 2})

    await store.prune("2026-06-01T00:00:00+00:00")

    remaining = await store.get_sync_cycles("2000-01-01T00:00:00+00:00", limit=100)
    assert [r.finished_at for r in remaining] == [T1100]

    _, mem_rows = await store.get_latest_memory()
    assert [(r.domain, r.memory_bytes) for r in mem_rows] == [("satellite", 20)]

    info_history = await store.get_info_history("2000-01-01T00:00:00+00:00")
    assert [h.used_memory for h in info_history] == [2]
