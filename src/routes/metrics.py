"""Status & performance dashboard endpoints for the data-service.

Serves per-domain sync history and Redis memory-by-domain metrics from the
SQLite :class:`MetricsStore` (written by the sync services + the background
:class:`RedisMetricsService`), plus live basemap scraper state.
"""

import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, Query

from clients.basemap_state_store import BasemapStateStore
from clients.metrics_store import InfoSample, MetricsStore
from clients.redis_client import RedisClient
from dependencies import (
    get_basemap_state_store,
    get_metrics_store,
    get_redis_client,
    get_settings,
)
from models.metrics import (
    BasemapProviderStatus,
    MemoryDomain,
    MemoryHistoryPoint,
    MetricsSummary,
    RedisInfo,
    RedisMemoryResponse,
    SyncCycle,
    SyncDomainStatus,
    SyncHistoryPoint,
    SyncStatusResponse,
)
from services.basemap_config import load_providers
from settings import Settings

router = APIRouter(prefix="/metrics", tags=["Metrics"])

_EPOCH = "1970-01-01T00:00:00+00:00"


def _since(hours: int) -> str:
    """ISO8601 lower bound for a lookback window; hours<=0 means all time."""
    if hours <= 0:
        return _EPOCH
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def _info_from_sample(sample: InfoSample) -> RedisInfo:
    """InfoSample and RedisInfo share field names; map straight across."""
    return RedisInfo(**asdict(sample))


def _info_from_raw(
    raw: Dict[str, object], total_keys: int, sampled_at: str
) -> RedisInfo:
    """Build a RedisInfo from a live INFO dict + DBSIZE."""
    return RedisInfo(
        sampled_at=sampled_at,
        used_memory=raw.get("used_memory"),  # type: ignore[arg-type]
        used_memory_rss=raw.get("used_memory_rss"),  # type: ignore[arg-type]
        used_memory_peak=raw.get("used_memory_peak"),  # type: ignore[arg-type]
        maxmemory=raw.get("maxmemory"),  # type: ignore[arg-type]
        mem_fragmentation_ratio=raw.get("mem_fragmentation_ratio"),  # type: ignore[arg-type]
        evicted_keys=raw.get("evicted_keys"),  # type: ignore[arg-type]
        expired_keys=raw.get("expired_keys"),  # type: ignore[arg-type]
        keyspace_hits=raw.get("keyspace_hits"),  # type: ignore[arg-type]
        keyspace_misses=raw.get("keyspace_misses"),  # type: ignore[arg-type]
        connected_clients=raw.get("connected_clients"),  # type: ignore[arg-type]
        total_keys=total_keys,
    )


@router.get("/summary", response_model=MetricsSummary, summary="Dashboard KPIs")
async def get_summary(
    metrics_store: MetricsStore = Depends(get_metrics_store),
    redis_client: RedisClient = Depends(get_redis_client),
) -> MetricsSummary:
    """Top-level numbers for the header cards: Redis memory + sync health."""
    sampled_at, memory = await metrics_store.get_latest_memory()
    info = await metrics_store.get_latest_info()
    raw_sync = await redis_client.get_sync_status()
    domains = await metrics_store.get_latest_sync_per_domain()

    total_bytes = sum(d.memory_bytes for d in memory)
    top = memory[0] if memory else None
    total_keys = (
        info.total_keys
        if info and info.total_keys is not None
        else sum(d.key_count for d in memory)
    )
    last_finished = max((d.finished_at for d in domains), default=None)
    return MetricsSummary(
        sampled_at=sampled_at,
        used_memory=info.used_memory if info else None,
        used_memory_rss=info.used_memory_rss if info else None,
        maxmemory=info.maxmemory if info else None,
        total_keys=total_keys or 0,
        total_bytes=total_bytes,
        top_domain=top.domain if top else None,
        top_domain_bytes=top.memory_bytes if top else 0,
        sync_is_running=raw_sync.get("is_running", "false") == "true",
        sync_total_cycles=int(raw_sync.get("total_cycles", "0") or 0),
        sync_consecutive_failures=int(raw_sync.get("consecutive_failures", "0") or 0),
        active_sync_domains=len(domains),
        last_sync_finished=last_finished,
    )


@router.get(
    "/sync/status", response_model=SyncStatusResponse, summary="Per-domain sync status"
)
async def get_sync_status(
    metrics_store: MetricsStore = Depends(get_metrics_store),
    redis_client: RedisClient = Depends(get_redis_client),
) -> SyncStatusResponse:
    """Latest cycle per domain plus the combined sync-loop running flags."""
    raw = await redis_client.get_sync_status()
    rows = await metrics_store.get_latest_sync_per_domain()

    def _float(key: str) -> Optional[float]:
        value = raw.get(key)
        return float(value) if value else None

    return SyncStatusResponse(
        is_running=raw.get("is_running", "false") == "true",
        total_cycles=int(raw.get("total_cycles", "0") or 0),
        consecutive_failures=int(raw.get("consecutive_failures", "0") or 0),
        last_sync_start=_float("last_sync_start"),
        last_sync_end=_float("last_sync_end"),
        domains=[
            SyncDomainStatus(
                domain=r.domain,
                last_started=r.started_at,
                last_finished=r.finished_at,
                last_duration_ms=r.duration_ms,
                last_downloaded=r.downloaded,
                last_errors=r.errors,
                outcome=r.outcome,
            )
            for r in rows
        ],
    )


@router.get(
    "/sync/history",
    response_model=List[SyncHistoryPoint],
    summary="Sync throughput/error trend",
)
async def get_sync_history(
    hours: int = Query(24, ge=0),
    bucket: str = Query("hour", pattern="^(hour|day)$"),
    domain: Optional[str] = None,
    metrics_store: MetricsStore = Depends(get_metrics_store),
) -> List[SyncHistoryPoint]:
    """Per-domain cycle counts / downloads / errors aggregated into time buckets."""
    rows = await metrics_store.get_sync_history(
        _since(hours), bucket=bucket, domain=domain
    )
    return [
        SyncHistoryPoint(
            bucket=r.bucket,
            domain=r.domain,
            cycles=r.cycles,
            downloaded=r.downloaded,
            errors=r.errors,
            avg_duration_ms=r.avg_duration_ms,
        )
        for r in rows
    ]


@router.get(
    "/sync/cycles", response_model=List[SyncCycle], summary="Recent sync cycles"
)
async def get_sync_cycles(
    hours: int = Query(24, ge=0),
    domain: Optional[str] = None,
    limit: int = Query(200, ge=1, le=1000),
    metrics_store: MetricsStore = Depends(get_metrics_store),
) -> List[SyncCycle]:
    """Raw recent cycle rows, newest first, for the cycles table."""
    rows = await metrics_store.get_sync_cycles(
        _since(hours), domain=domain, limit=limit
    )
    return [
        SyncCycle(
            domain=r.domain,
            started_at=r.started_at,
            finished_at=r.finished_at,
            duration_ms=r.duration_ms,
            downloaded=r.downloaded,
            errors=r.errors,
            outcome=r.outcome,
        )
        for r in rows
    ]


@router.get(
    "/redis/memory",
    response_model=RedisMemoryResponse,
    summary="Redis memory by domain",
)
async def get_redis_memory(
    metrics_store: MetricsStore = Depends(get_metrics_store),
) -> RedisMemoryResponse:
    """Latest memory-by-domain breakdown (the dashboard's headline panel)."""
    sampled_at, rows = await metrics_store.get_latest_memory()
    return RedisMemoryResponse(
        sampled_at=sampled_at,
        total_keys=sum(r.key_count for r in rows),
        total_bytes=sum(r.memory_bytes for r in rows),
        domains=[
            MemoryDomain(
                domain=r.domain, key_count=r.key_count, memory_bytes=r.memory_bytes
            )
            for r in rows
        ],
    )


@router.get(
    "/redis/memory/history",
    response_model=List[MemoryHistoryPoint],
    summary="Redis memory growth over time",
)
async def get_redis_memory_history(
    hours: int = Query(168, ge=0),
    domain: Optional[str] = None,
    metrics_store: MetricsStore = Depends(get_metrics_store),
) -> List[MemoryHistoryPoint]:
    """Memory-by-domain time series for the growth chart."""
    rows = await metrics_store.get_memory_history(_since(hours), domain=domain)
    return [
        MemoryHistoryPoint(
            sampled_at=r.sampled_at,
            domain=r.domain,
            key_count=r.key_count,
            memory_bytes=r.memory_bytes,
        )
        for r in rows
    ]


@router.get("/redis/info", response_model=RedisInfo, summary="Redis INFO snapshot")
async def get_redis_info(
    live: bool = False,
    metrics_store: MetricsStore = Depends(get_metrics_store),
    redis_client: RedisClient = Depends(get_redis_client),
) -> RedisInfo:
    """Latest stored Redis INFO snapshot, or a live one when `live=true`."""
    if live:
        raw = await redis_client.info()
        total_keys = await redis_client.dbsize()
        return _info_from_raw(raw, total_keys, datetime.now(timezone.utc).isoformat())
    sample = await metrics_store.get_latest_info()
    return _info_from_sample(sample) if sample else RedisInfo()


@router.get(
    "/redis/info/history",
    response_model=List[RedisInfo],
    summary="Redis INFO over time",
)
async def get_redis_info_history(
    hours: int = Query(168, ge=0),
    metrics_store: MetricsStore = Depends(get_metrics_store),
) -> List[RedisInfo]:
    """Redis INFO snapshots for the used_memory / fragmentation trend charts."""
    return [
        _info_from_sample(s)
        for s in await metrics_store.get_info_history(_since(hours))
    ]


@router.get(
    "/basemap/providers",
    response_model=List[BasemapProviderStatus],
    summary="Basemap scraper per-provider state",
)
async def get_basemap_providers(
    settings: Settings = Depends(get_settings),
    state_store: Optional[BasemapStateStore] = Depends(get_basemap_state_store),
) -> List[BasemapProviderStatus]:
    """Per-provider scrape cursor, last completion and circuit-breaker state."""
    providers = load_providers(settings.basemap_providers)
    now = int(time.time())
    result: List[BasemapProviderStatus] = []
    for provider_id, provider in providers.items():
        cursor = None
        last_completed = None
        health = None
        stats = None
        if state_store is not None:
            cursor = await state_store.get_cursor(provider_id)
            last_completed = await state_store.get_last_completed(provider_id)
            health = await state_store.get_health(provider_id)
            stats = await state_store.get_scrape_stats(provider_id)
        error_rate = (
            stats.failed / stats.attempted if stats and stats.attempted else None
        )
        result.append(
            BasemapProviderStatus(
                provider_id=provider_id,
                name=provider.name,
                min_zoom=provider.min_zoom,
                max_zoom=provider.cache_max_zoom,
                in_progress=cursor is not None,
                cursor_zoom=cursor.zoom if cursor else None,
                cursor_tile_index=cursor.tile_index if cursor else None,
                last_completed=last_completed,
                circuit_open=bool(health and health.cooldown_until > now),
                consecutive_trips=health.consecutive_trips if health else 0,
                cooldown_until=health.cooldown_until if health else None,
                last_reason=health.last_reason if health else None,
                attempted=stats.attempted if stats else 0,
                ok=stats.ok if stats else 0,
                failed=stats.failed if stats else 0,
                error_rate=error_rate,
                completed=stats.completed if stats else False,
                last_swept=stats.swept_at if stats else None,
            )
        )
    return result
