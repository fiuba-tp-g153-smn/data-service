"""Endpoint tests for the /metrics/* status dashboard routes."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

import routes.metrics as metrics_routes
from clients.basemap_state_store import Cursor, ProviderHealth, ScrapeStats
from clients.metrics_store import (
    InfoSample,
    MemoryDomainSample,
    MemorySamplePoint,
    SyncCycleRow,
    SyncHistoryBucket,
)
from dependencies import (
    get_basemap_state_store,
    get_metrics_store,
    get_redis_client,
    get_settings,
)
from main import app


def _sync_row(domain="satellite", downloaded=10, errors=0, outcome="ok"):
    return SyncCycleRow(
        domain=domain,
        started_at="2026-06-07T10:00:00+00:00",
        finished_at="2026-06-07T10:00:05+00:00",
        duration_ms=5000,
        downloaded=downloaded,
        errors=errors,
        outcome=outcome,
    )


def test_sync_status_merges_domains_and_flags():
    store = AsyncMock()
    store.get_latest_sync_per_domain = AsyncMock(
        return_value=[_sync_row("satellite"), _sync_row("radar", downloaded=3)]
    )
    redis = AsyncMock()
    redis.get_sync_status = AsyncMock(
        return_value={
            "is_running": "true",
            "total_cycles": "42",
            "consecutive_failures": "1",
            "last_sync_start": "1706000000.0",
            "last_sync_end": "1706000005.0",
        }
    )
    app.dependency_overrides[get_metrics_store] = lambda: store
    app.dependency_overrides[get_redis_client] = lambda: redis
    try:
        data = TestClient(app).get("/metrics/sync/status").json()
        assert data["is_running"] is True
        assert data["total_cycles"] == 42
        assert data["consecutive_failures"] == 1
        assert {d["domain"] for d in data["domains"]} == {"satellite", "radar"}
        sat = next(d for d in data["domains"] if d["domain"] == "satellite")
        assert sat["last_downloaded"] == 10
        assert sat["outcome"] == "ok"
    finally:
        app.dependency_overrides.clear()


def test_redis_memory_totals_and_breakdown():
    store = AsyncMock()
    store.get_latest_memory = AsyncMock(
        return_value=(
            "2026-06-07T10:00:00+00:00",
            [
                MemoryDomainSample("basemap", 100, 900),
                MemoryDomainSample("satellite", 40, 400),
            ],
        )
    )
    app.dependency_overrides[get_metrics_store] = lambda: store
    try:
        data = TestClient(app).get("/metrics/redis/memory").json()
        assert data["sampled_at"] == "2026-06-07T10:00:00+00:00"
        assert data["total_bytes"] == 1300
        assert data["total_keys"] == 140
        assert data["domains"][0]["domain"] == "basemap"
    finally:
        app.dependency_overrides.clear()


def test_redis_memory_empty_when_no_sample():
    store = AsyncMock()
    store.get_latest_memory = AsyncMock(return_value=(None, []))
    app.dependency_overrides[get_metrics_store] = lambda: store
    try:
        data = TestClient(app).get("/metrics/redis/memory").json()
        assert data["sampled_at"] is None
        assert data["total_bytes"] == 0
        assert data["domains"] == []
    finally:
        app.dependency_overrides.clear()


def test_redis_info_stored():
    store = AsyncMock()
    store.get_latest_info = AsyncMock(
        return_value=InfoSample(
            sampled_at="2026-06-07T10:00:00+00:00",
            used_memory=12345,
            used_memory_rss=20000,
            used_memory_peak=30000,
            maxmemory=0,
            mem_fragmentation_ratio=1.2,
            evicted_keys=0,
            expired_keys=5,
            keyspace_hits=100,
            keyspace_misses=10,
            connected_clients=3,
            total_keys=140,
        )
    )
    app.dependency_overrides[get_metrics_store] = lambda: store
    try:
        data = TestClient(app).get("/metrics/redis/info").json()
        assert data["used_memory"] == 12345
        assert data["total_keys"] == 140
        assert data["mem_fragmentation_ratio"] == 1.2
    finally:
        app.dependency_overrides.clear()


def test_redis_info_live_queries_redis():
    store = AsyncMock()
    redis = AsyncMock()
    redis.info = AsyncMock(return_value={"used_memory": 555, "evicted_keys": 2})
    redis.dbsize = AsyncMock(return_value=9)
    app.dependency_overrides[get_metrics_store] = lambda: store
    app.dependency_overrides[get_redis_client] = lambda: redis
    try:
        data = TestClient(app).get("/metrics/redis/info?live=true").json()
        assert data["used_memory"] == 555
        assert data["evicted_keys"] == 2
        assert data["total_keys"] == 9
        assert data["sampled_at"] is not None
        store.get_latest_info.assert_not_called()
    finally:
        app.dependency_overrides.clear()


def test_summary_combines_memory_info_and_sync():
    store = AsyncMock()
    store.get_latest_memory = AsyncMock(
        return_value=(
            "2026-06-07T10:00:00+00:00",
            [
                MemoryDomainSample("basemap", 100, 900),
                MemoryDomainSample("radar", 5, 50),
            ],
        )
    )
    store.get_latest_info = AsyncMock(
        return_value=InfoSample(
            sampled_at="2026-06-07T10:00:00+00:00",
            used_memory=2048,
            used_memory_rss=4096,
            used_memory_peak=None,
            maxmemory=None,
            mem_fragmentation_ratio=None,
            evicted_keys=None,
            expired_keys=None,
            keyspace_hits=None,
            keyspace_misses=None,
            connected_clients=None,
            total_keys=105,
        )
    )
    store.get_latest_sync_per_domain = AsyncMock(
        return_value=[_sync_row("satellite"), _sync_row("radar")]
    )
    redis = AsyncMock()
    redis.get_sync_status = AsyncMock(
        return_value={"is_running": "false", "total_cycles": "7"}
    )
    app.dependency_overrides[get_metrics_store] = lambda: store
    app.dependency_overrides[get_redis_client] = lambda: redis
    try:
        data = TestClient(app).get("/metrics/summary").json()
        assert data["used_memory"] == 2048
        assert data["total_keys"] == 105
        assert data["total_bytes"] == 950
        assert data["top_domain"] == "basemap"
        assert data["top_domain_bytes"] == 900
        assert data["sync_total_cycles"] == 7
        assert data["active_sync_domains"] == 2
        assert data["last_sync_finished"] == "2026-06-07T10:00:05+00:00"
    finally:
        app.dependency_overrides.clear()


def test_sync_history_maps_buckets():
    store = AsyncMock()
    store.get_sync_history = AsyncMock(
        return_value=[
            SyncHistoryBucket("2026-06-07T10", "satellite", 2, 12, 1, 200.0),
        ]
    )
    app.dependency_overrides[get_metrics_store] = lambda: store
    try:
        data = TestClient(app).get("/metrics/sync/history?hours=24&bucket=hour").json()
        assert data == [
            {
                "bucket": "2026-06-07T10",
                "domain": "satellite",
                "cycles": 2,
                "downloaded": 12,
                "errors": 1,
                "avg_duration_ms": 200.0,
            }
        ]
    finally:
        app.dependency_overrides.clear()


def test_sync_history_rejects_bad_bucket():
    store = AsyncMock()
    app.dependency_overrides[get_metrics_store] = lambda: store
    try:
        resp = TestClient(app).get("/metrics/sync/history?bucket=week")
        assert resp.status_code == 422
    finally:
        app.dependency_overrides.clear()


def test_sync_cycles_passes_since_before_and_unlimited():
    store = AsyncMock()
    store.get_sync_cycles = AsyncMock(return_value=[])
    app.dependency_overrides[get_metrics_store] = lambda: store
    try:
        TestClient(app).get(
            "/metrics/sync/cycles"
            "?since=2026-06-01T00:00:00%2B00:00&before=2026-06-02T00:00:00%2B00:00&limit=0"
        )
        store.get_sync_cycles.assert_awaited_once()
        args, kwargs = store.get_sync_cycles.call_args
        assert args[0] == "2026-06-01T00:00:00+00:00"  # since overrides hours
        assert kwargs["before_iso"] == "2026-06-02T00:00:00+00:00"
        assert kwargs["limit"] == 0
    finally:
        app.dependency_overrides.clear()


def test_memory_history_maps_points():
    store = AsyncMock()
    store.get_memory_history = AsyncMock(
        return_value=[
            MemorySamplePoint("2026-06-07T10:00:00+00:00", "basemap", 10, 100),
        ]
    )
    app.dependency_overrides[get_metrics_store] = lambda: store
    try:
        data = TestClient(app).get("/metrics/redis/memory/history?hours=24").json()
        assert data[0]["domain"] == "basemap"
        assert data[0]["memory_bytes"] == 100
    finally:
        app.dependency_overrides.clear()


def _provider(provider_id="argenmap"):
    return SimpleNamespace(
        provider_id=provider_id,
        name="Argenmap",
        min_zoom=3,
        cache_max_zoom=11,
    )


def test_basemap_providers_with_state(monkeypatch):
    monkeypatch.setattr(
        metrics_routes, "load_providers", lambda _cfg: {"argenmap": _provider()}
    )
    state = AsyncMock()
    state.get_cursor = AsyncMock(return_value=Cursor(zoom=9, tile_index=1234))
    state.get_last_completed = AsyncMock(return_value=1700000000)
    state.get_health = AsyncMock(
        return_value=ProviderHealth(
            consecutive_trips=2,
            cooldown_until=9999999999,
            last_tripped_at=1700000000,
            last_reason="boom",
        )
    )
    state.get_scrape_stats = AsyncMock(
        return_value=ScrapeStats(
            attempted=1000, ok=997, failed=3, completed=False, swept_at=1700000000
        )
    )
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(
        basemap_providers=[]
    )
    app.dependency_overrides[get_basemap_state_store] = lambda: state
    try:
        data = TestClient(app).get("/metrics/basemap/providers").json()
        assert len(data) == 1
        row = data[0]
        assert row["provider_id"] == "argenmap"
        assert row["in_progress"] is True
        assert row["cursor_zoom"] == 9
        assert row["circuit_open"] is True
        assert row["consecutive_trips"] == 2
        assert row["last_reason"] == "boom"
        assert row["attempted"] == 1000
        assert row["failed"] == 3
        assert row["error_rate"] == 3 / 1000
        assert row["completed"] is False
        assert row["last_swept"] == 1700000000
    finally:
        app.dependency_overrides.clear()


def test_basemap_providers_without_state_store(monkeypatch):
    monkeypatch.setattr(
        metrics_routes, "load_providers", lambda _cfg: {"argenmap": _provider()}
    )
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(
        basemap_providers=[]
    )
    app.dependency_overrides[get_basemap_state_store] = lambda: None
    try:
        data = TestClient(app).get("/metrics/basemap/providers").json()
        assert len(data) == 1
        assert data[0]["in_progress"] is False
        assert data[0]["circuit_open"] is False
        assert data[0]["consecutive_trips"] == 0
        assert data[0]["attempted"] == 0
        assert data[0]["failed"] == 0
        assert data[0]["error_rate"] is None
    finally:
        app.dependency_overrides.clear()
