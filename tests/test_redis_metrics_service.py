"""Unit tests for `RedisMetricsService` (Redis memory-by-domain collector)."""

from types import SimpleNamespace

import pytest
import pytest_asyncio

from clients.metrics_store import MetricsStore
from db.migrate import run_migrations
from services.redis_metrics_service import RedisMetricsService, classify_key


def test_classify_key_maps_known_prefixes():
    assert classify_key(b"tile:sat:band_2/ts/3/1/1") == "satellite"
    assert classify_key(b"tile:radar:r/v/t/3/1/1") == "radar"
    assert classify_key(b"tile:ecmwf_tp:f/p/3/1/1") == "ecmwf_tp"
    assert classify_key(b"geojson:ecmwf_mslp:f/t") == "ecmwf_mslp"
    assert classify_key(b"tile:wrf:p/init/f/3/1/1") == "wrf"
    assert classify_key(b"geojson:wrf:p/init/f/layer") == "wrf"
    assert classify_key(b"tile:basemap:argenmap:3:1:1") == "basemap"
    assert classify_key(b"tile:basemap:miss:argenmap:3:1:1") == "basemap"
    assert classify_key(b"basemap:availability:argenmap") == "basemap"
    assert classify_key(b"cache:ws:latest") == "weather_stations"
    assert classify_key(b"idx:sat:band_2") == "indexes"
    assert classify_key(b"cache:radar:listing") == "listings"
    assert classify_key(b"sync:status") == "sync"


def test_classify_key_unknown_prefix_is_other():
    assert classify_key(b"whatever:else") == "other"
    assert classify_key(b"\xff\xfe-binary-ish") == "other"


def _settings(**overrides):
    base = dict(
        redis_metrics_sample_interval_seconds=900,
        redis_metrics_scan_count=1000,
        redis_metrics_memory_batch_size=2,  # small to exercise batch flushing
        metrics_retention_days=14,
        metrics_max_rows=1_000_000,
        metrics_lock_path="/tmp/test_redis_metrics.lock",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class _FakeRedis:
    """Minimal async stand-in exposing the introspection primitives used."""

    def __init__(self, sizes, info, dbsize):
        self._sizes = sizes  # dict[bytes, Optional[int]]
        self._info = info
        self._dbsize = dbsize

    async def scan_keys(self, match=None, count=1000):
        for key in self._sizes:
            yield key

    async def memory_usage_batch(self, keys):
        return [self._sizes[key] for key in keys]

    async def info(self, section=None):
        return self._info

    async def dbsize(self):
        return self._dbsize


@pytest_asyncio.fixture
async def store(tmp_path):
    db_path = tmp_path / "metrics.sqlite"
    run_migrations(db_path)  # schema is Alembic-owned; connect no longer creates it
    s = MetricsStore(str(db_path))
    await s.connect()
    try:
        yield s
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_run_sync_aggregates_memory_by_domain(store):
    sizes = {
        b"tile:sat:band_2/ts/3/1/1": 100,
        b"tile:sat:band_2/ts/3/1/2": 150,
        b"tile:radar:r/v/t/3/1/1": 200,
        b"idx:sat:band_2": 50,
        b"cache:ws:latest": 30,
        b"sync:status": None,  # key vanished mid-scan -> counts, 0 bytes
        b"weirdkey": 10,
    }
    redis = _FakeRedis(sizes, info={"used_memory": 9999}, dbsize=7)
    svc = RedisMetricsService(_settings(), redis, store)

    await svc._run_sync()  # pylint: disable=protected-access

    sampled_at, rows = await store.get_latest_memory()
    assert sampled_at is not None
    by_domain = {r.domain: (r.key_count, r.memory_bytes) for r in rows}
    assert by_domain == {
        "satellite": (2, 250),
        "radar": (1, 200),
        "indexes": (1, 50),
        "weather_stations": (1, 30),
        "sync": (1, 0),
        "other": (1, 10),
    }


@pytest.mark.asyncio
async def test_run_sync_records_info_snapshot(store):
    redis = _FakeRedis(
        {b"tile:sat:x": 100},
        info={
            "used_memory": 12345,
            "used_memory_rss": 20000,
            "mem_fragmentation_ratio": 1.2,
            "evicted_keys": 3,
        },
        dbsize=42,
    )
    svc = RedisMetricsService(_settings(), redis, store)

    await svc._run_sync()  # pylint: disable=protected-access

    info = await store.get_latest_info()
    assert info is not None
    assert info.used_memory == 12345
    assert info.used_memory_rss == 20000
    assert info.mem_fragmentation_ratio == 1.2
    assert info.evicted_keys == 3
    assert info.total_keys == 42  # comes from dbsize(), not info()


@pytest.mark.asyncio
async def test_run_sync_lock_path_from_settings(store):
    redis = _FakeRedis({b"tile:sat:x": 1}, info={}, dbsize=1)
    svc = RedisMetricsService(
        _settings(metrics_lock_path="/tmp/custom.lock"), redis, store
    )
    assert (
        svc._get_lock_path() == "/tmp/custom.lock"
    )  # pylint: disable=protected-access
