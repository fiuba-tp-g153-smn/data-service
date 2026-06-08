"""
Background collector that samples Redis memory usage by domain.

Redis has no native memory-by-prefix breakdown, so each cycle SCANs the whole
keyspace, classifies every key into a domain bucket by its key prefix, and runs
``MEMORY USAGE`` per key (pipelined) to sum bytes + counts per domain. The result
plus an overall ``INFO`` snapshot are written to :class:`MetricsStore` under a
single timestamp, giving the dashboard a memory-growth-over-time series that
pinpoints which domain is consuming memory.

Accurate but O(N keys), so it runs on a generous interval and only on one worker
(``fcntl`` lock via :class:`BaseSyncService`).
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from clients.metrics_store import MetricsStore
from clients.redis_client import RedisClient
from services.base_sync_service import BaseSyncService
from settings import Settings

logger = logging.getLogger(__name__)

# Key-prefix → dashboard domain. Ordered: the first matching prefix wins, so
# more specific prefixes must precede broader ones (e.g. cache:ws: before cache:).
_PREFIX_DOMAINS = (
    ("tile:sat:", "satellite"),
    ("tile:radar:", "radar"),
    ("tile:ecmwf_tp:", "ecmwf_tp"),
    ("geojson:ecmwf_mslp:", "ecmwf_mslp"),
    ("tile:wrf:", "wrf"),
    ("geojson:wrf:", "wrf"),
    ("tile:basemap:", "basemap"),
    ("basemap:availability:", "basemap"),
    ("cache:ws:", "weather_stations"),
    ("idx:", "indexes"),
    ("cache:", "listings"),
    ("sync:", "sync"),
)
_DOMAIN_OTHER = "other"


def classify_key(key: bytes) -> str:
    """Map a raw Redis key to its dashboard domain by prefix."""
    name = key.decode("utf-8", "replace")
    for prefix, domain in _PREFIX_DOMAINS:
        if name.startswith(prefix):
            return domain
    return _DOMAIN_OTHER


class RedisMetricsService(BaseSyncService):
    """Periodically snapshots Redis memory-by-domain into the metrics store."""

    def __init__(
        self,
        settings: Settings,
        redis_client: RedisClient,
        metrics_store: MetricsStore,
    ):
        super().__init__(
            settings=settings,
            sync_interval=settings.redis_metrics_sample_interval_seconds,
            service_name="Redis metrics collector",
        )
        self._redis_client = redis_client
        self._metrics_store = metrics_store

    def _get_lock_path(self) -> str:
        return self._settings.metrics_lock_path

    async def _run_sync(self) -> None:
        """Collect one memory + INFO sample. Errors bubble to the base loop."""
        counts: Dict[str, int] = defaultdict(int)
        memory: Dict[str, int] = defaultdict(int)
        batch: List[bytes] = []
        batch_domains: List[str] = []
        batch_size = self._settings.redis_metrics_memory_batch_size

        async for key in self._redis_client.scan_keys(
            count=self._settings.redis_metrics_scan_count
        ):
            domain = classify_key(key)
            counts[domain] += 1
            batch.append(key)
            batch_domains.append(domain)
            if len(batch) >= batch_size:
                await self._flush_batch(batch, batch_domains, memory)
                batch.clear()
                batch_domains.clear()

        if batch:
            await self._flush_batch(batch, batch_domains, memory)

        sampled_at = datetime.now(timezone.utc).isoformat()
        rows = [
            (domain, count, memory.get(domain, 0)) for domain, count in counts.items()
        ]
        await self._metrics_store.record_memory_sample(sampled_at, rows)
        await self._metrics_store.record_info_sample(
            sampled_at, await self._collect_info()
        )

        cutoff = (
            datetime.now(timezone.utc)
            - timedelta(days=self._settings.metrics_retention_days)
        ).isoformat()
        await self._metrics_store.prune(cutoff)
        # Backstop behind time-based retention: cap each table's row count.
        await self._metrics_store.prune_to_max_rows(self._settings.metrics_max_rows)

        logger.info(
            "Redis metrics sample: %d keys / %d bytes across %d domains",
            sum(counts.values()),
            sum(memory.values()),
            len(counts),
        )

    async def _flush_batch(
        self, keys: List[bytes], domains: List[str], memory: Dict[str, int]
    ) -> None:
        """Sum MEMORY USAGE for one pipelined batch into the per-domain totals."""
        sizes = await self._redis_client.memory_usage_batch(keys)
        for domain, size in zip(domains, sizes):
            if size:
                memory[domain] += size

    async def _collect_info(self) -> Dict[str, object]:
        """Snapshot Redis INFO + key count. MetricsStore extracts the fields it
        persists, so passing the raw INFO dict (plus total_keys) is enough."""
        info: Dict[str, object] = dict(await self._redis_client.info())
        info["total_keys"] = await self._redis_client.dbsize()
        return info
