"""Base map tile reader with 3-tier cache fallback (Redis → S3 → provider)."""

import asyncio
import logging
from typing import Optional

import httpx

from clients.http_tile_client import HttpTileClient
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.basemap_config import BasemapProvider, build_source_url

logger = logging.getLogger(__name__)


class BasemapTileReader:
    """
    Read a basemap tile using a 3-tier cache fallback:

      Tier 1 — Redis (hot cache, TTL'd)
      Tier 2 — SeaweedFS S3 (cold backup populated by the scraper)
      Tier 3 — External provider (proxy fallback for misses before the scraper
               has populated this tile; results are fed back into Tiers 1+2).

    The scraper is the source of truth: this reader is what routes call on a
    per-request basis. Cache-miss writes back to Redis/S3 are scheduled as
    background tasks, throttled by `cache_semaphore` to avoid unbounded task
    pile-up under load.
    """

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: S3Client,
        http_client: HttpTileClient,
        providers: dict[str, BasemapProvider],
        tile_ttl: int,
        cache_concurrent: int,
    ):
        self._redis = redis_client
        self._s3 = s3_client
        self._http = http_client
        self._providers = providers
        self._tile_ttl = tile_ttl
        self._cache_semaphore = asyncio.Semaphore(cache_concurrent)
        self._inflight_cache_tasks: set[asyncio.Task] = set()

    async def get_tile(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Fetch a tile with Redis → S3 → external-provider fallback."""
        data = await self._redis.get_basemap_tile(provider_id, z, x, y)
        if data:
            return data

        s3_key = S3Client.build_basemap_tile_key(provider_id, z, x, y)

        data = await self._s3.download_tile(s3_key)
        if data:
            self._schedule_cache_write(
                self._redis.store_basemap_tile(
                    provider_id, z, x, y, data, ttl=self._tile_ttl
                ),
                label=f"redis-after-s3 {s3_key}",
            )
            return data

        provider = self._providers.get(provider_id)
        if not provider:
            return None

        url = build_source_url(provider, z, x, y)
        data = await self._http.download_tile(url)
        if data:
            self._schedule_cache_write(
                self._cache_tile(provider_id, z, x, y, s3_key, data),
                label=f"s3+redis-after-provider {s3_key}",
            )
            return data

        return None

    def _schedule_cache_write(self, coro, label: str) -> None:
        """Spawn a throttled background cache write and track it."""
        task = asyncio.create_task(self._run_throttled(coro, label))
        self._inflight_cache_tasks.add(task)
        task.add_done_callback(self._inflight_cache_tasks.discard)

    async def _run_throttled(self, coro, label: str) -> None:
        """Acquire the cache semaphore, then run the coroutine with narrow error handling."""
        async with self._cache_semaphore:
            try:
                await coro
            except (httpx.HTTPError, asyncio.TimeoutError, OSError) as exc:
                logger.warning("Cache write failed (%s): %s", label, exc)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Unexpected cache-write failure (%s): %s", label, exc, exc_info=True
                )

    async def _cache_tile(
        self, provider_id: str, z: int, x: int, y: int, s3_key: str, data: bytes
    ) -> None:
        """Persist a freshly-fetched tile into both S3 and Redis."""
        await self._s3.upload_tile(s3_key, data)
        await self._redis.store_basemap_tile(
            provider_id, z, x, y, data, ttl=self._tile_ttl
        )

    async def close(self, timeout: float = 5.0) -> None:
        """Drain in-flight cache writes before shutdown."""
        if not self._inflight_cache_tasks:
            return
        logger.info(
            "Draining %d in-flight basemap cache writes...",
            len(self._inflight_cache_tasks),
        )
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._inflight_cache_tasks, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Timed out draining basemap cache writes after %.1fs", timeout
            )
