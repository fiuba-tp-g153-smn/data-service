"""
Background collector that samples Redis memory usage by domain.

Redis has no native memory-by-prefix breakdown, so each cycle SCANs the whole
keyspace and classifies every key into a domain bucket by its key prefix —
per-domain key counts are exact. ``MEMORY USAGE`` (pipelined) however runs only
on a uniform reservoir of up to ``redis_metrics_memory_sample_per_domain`` keys
per domain (Algorithm R), and the domain's bytes are extrapolated as
``mean(sample) * count`` — key sizes are near-homogeneous within a domain, so
the estimate lands within a few percent at a fraction of the command volume.
Setting the cap to 0 restores the exact per-key census. Keys that vanish
between SCAN and measurement count as 0 bytes. The result plus an overall
``INFO`` snapshot are written to :class:`MetricsStore` under a single
timestamp, giving the dashboard a memory-growth-over-time series that
pinpoints which domain is consuming memory.

Runs on a generous interval and only on one worker (``fcntl`` lock via
:class:`BaseSyncService`).
"""

import logging
import random
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


def _reservoir_observe(sample: List[bytes], key: bytes, seen: int, cap: int) -> None:
    """Algorithm R: keep a uniform sample of <= ``cap`` keys from a stream.

    ``seen`` is the key's 1-indexed position in the stream; ``cap <= 0``
    keeps every key (exact census).
    """
    if cap <= 0 or len(sample) < cap:
        sample.append(key)
        return
    slot = random.randrange(seen)
    if slot < cap:
        sample[slot] = key


def _extrapolate(measured: int, sampled: int, total: int) -> int:
    """Scale sampled bytes to the domain's key count; exact when fully sampled."""
    if sampled >= total:
        return measured
    if sampled == 0:
        return 0
    return round(measured / sampled * total)


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
        reservoirs: Dict[str, List[bytes]] = defaultdict(list)
        cap = self._settings.redis_metrics_memory_sample_per_domain

        async for key in self._redis_client.scan_keys(
            count=self._settings.redis_metrics_scan_count
        ):
            domain = classify_key(key)
            counts[domain] += 1
            _reservoir_observe(reservoirs[domain], key, counts[domain], cap)

        memory: Dict[str, int] = {}
        sampled_keys = 0
        for domain, count in counts.items():
            sample = reservoirs[domain]
            sampled_keys += len(sample)
            memory[domain] = await self._measure_domain(sample, count)

        await self._record_sample(counts, memory, sampled_keys)

    async def _measure_domain(self, sample: List[bytes], total: int) -> int:
        """MEMORY USAGE a domain's sampled keys and extrapolate to its key count."""
        batch_size = self._settings.redis_metrics_memory_batch_size
        measured = 0
        for start in range(0, len(sample), batch_size):
            sizes = await self._redis_client.memory_usage_batch(
                sample[start : start + batch_size]
            )
            measured += sum(size or 0 for size in sizes)
        return _extrapolate(measured, len(sample), total)

    async def _record_sample(
        self, counts: Dict[str, int], memory: Dict[str, int], sampled_keys: int
    ) -> None:
        """Persist the per-domain rows + INFO snapshot and apply retention."""
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
            "Redis metrics sample: %d keys (%d measured) / %d bytes across %d domains",
            sum(counts.values()),
            sampled_keys,
            sum(memory.values()),
            len(counts),
        )

    async def _collect_info(self) -> Dict[str, object]:
        """Snapshot Redis INFO + key count. MetricsStore extracts the fields it
        persists, so passing the raw INFO dict (plus total_keys) is enough."""
        info: Dict[str, object] = dict(await self._redis_client.info())
        info["total_keys"] = await self._redis_client.dbsize()
        return info
