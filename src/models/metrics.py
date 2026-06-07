"""Response models for the data-service status/performance dashboard."""

from typing import List, Optional

from pydantic import BaseModel


class SyncDomainStatus(BaseModel):
    """Latest cycle for a single sync domain (its live status)."""

    domain: str
    last_started: Optional[str] = None
    last_finished: Optional[str] = None
    last_duration_ms: Optional[int] = None
    last_downloaded: Optional[int] = None
    last_errors: Optional[int] = None
    outcome: Optional[str] = None


class SyncStatusResponse(BaseModel):
    """Per-domain live status plus the combined sync-loop flags."""

    is_running: bool = False
    total_cycles: int = 0
    consecutive_failures: int = 0
    last_sync_start: Optional[float] = None
    last_sync_end: Optional[float] = None
    domains: List[SyncDomainStatus] = []


class SyncHistoryPoint(BaseModel):
    """One aggregated (time bucket, domain) point for the sync trend charts."""

    bucket: str
    domain: str
    cycles: int
    downloaded: int
    errors: int
    avg_duration_ms: float


class SyncCycle(BaseModel):
    """A single recorded sync cycle row (for the recent-cycles table)."""

    domain: str
    started_at: str
    finished_at: str
    duration_ms: int
    downloaded: int
    errors: int
    outcome: str


class MemoryDomain(BaseModel):
    """Redis memory usage for a single domain in the latest sample."""

    domain: str
    key_count: int
    memory_bytes: int


class RedisMemoryResponse(BaseModel):
    """Latest Redis memory-by-domain breakdown."""

    sampled_at: Optional[str] = None
    total_keys: int = 0
    total_bytes: int = 0
    domains: List[MemoryDomain] = []


class MemoryHistoryPoint(BaseModel):
    """One (timestamp, domain) memory data point for the growth chart."""

    sampled_at: str
    domain: str
    key_count: int
    memory_bytes: int


class RedisInfo(BaseModel):
    """A snapshot of overall Redis INFO stats."""

    sampled_at: Optional[str] = None
    used_memory: Optional[int] = None
    used_memory_rss: Optional[int] = None
    used_memory_peak: Optional[int] = None
    maxmemory: Optional[int] = None
    mem_fragmentation_ratio: Optional[float] = None
    evicted_keys: Optional[int] = None
    expired_keys: Optional[int] = None
    keyspace_hits: Optional[int] = None
    keyspace_misses: Optional[int] = None
    connected_clients: Optional[int] = None
    total_keys: Optional[int] = None


class BasemapProviderStatus(BaseModel):
    """Per-provider basemap scraper progress + circuit-breaker state."""

    provider_id: str
    name: str
    min_zoom: int
    max_zoom: int
    in_progress: bool = False
    cursor_zoom: Optional[int] = None
    cursor_tile_index: Optional[int] = None
    last_completed: Optional[int] = None
    circuit_open: bool = False
    consecutive_trips: int = 0
    cooldown_until: Optional[int] = None
    last_reason: Optional[str] = None


class MetricsSummary(BaseModel):
    """Top-level KPIs for the dashboard header cards."""

    sampled_at: Optional[str] = None
    used_memory: Optional[int] = None
    used_memory_rss: Optional[int] = None
    maxmemory: Optional[int] = None
    total_keys: int = 0
    total_bytes: int = 0
    top_domain: Optional[str] = None
    top_domain_bytes: int = 0
    sync_is_running: bool = False
    sync_total_cycles: int = 0
    sync_consecutive_failures: int = 0
    active_sync_domains: int = 0
    last_sync_finished: Optional[str] = None
