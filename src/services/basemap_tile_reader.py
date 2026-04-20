"""Base map tile reader with 3-tier cache fallback (Redis → S3 → provider)."""

import asyncio
import logging
from typing import Optional, Tuple

import httpx

from clients.http_tile_client import HttpTileClient, ProviderUnavailableError
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.basemap_config import BasemapProvider, build_source_url

logger = logging.getLogger(__name__)

_TileKey = Tuple[str, int, int, int]


class BasemapTileReader:
    # pylint: disable=too-many-instance-attributes
    """
    Read a basemap tile using a 3-tier cache fallback:

      Tier 1 — Redis (hot cache, TTL'd)
      Tier 2 — SeaweedFS S3 (cold backup populated by the scraper)
      Tier 3 — External provider (proxy fallback for misses; can be disabled
               via `online_fallback=False` to force fully-offline reads).

    The scraper is the source of truth: this reader is what routes call on a
    per-request basis. Cache-miss writes back to Redis/S3 are scheduled as
    background tasks, throttled by `cache_semaphore` to avoid unbounded task
    pile-up under load.

    Robustness additions:
      * Negative cache (Redis tombstone) for tiles that miss both S3 and the
        relay — prevents repeat probes for the same known-missing tile.
      * Single-flight in-flight dedup keyed on `(provider_id, z, x, y)` so a
        burst of concurrent requests collapses to one S3+relay round-trip.

    Mode behaviour is driven by three independent knobs configured by
    `main.configure_basemap`:
      * `redis_cache_enabled=False` skips every Redis interaction in the
        read path (no GET, no write-through, no tombstone). Used by
        `basemap_sync_mode = "no_cache"` and `"relay_only"` to keep RAM
        flat.
      * `s3_cache_enabled=False` skips the S3 probe and the S3 upload
        after a relay hit. Used by `"relay_only"` so the reader becomes a
        pure provider proxy.
      * `negative_cache_enabled=False` disables the Redis tombstone used to
        suppress repeat probes of known-missing tiles. Force-off whenever
        Redis is off (tombstones are Redis writes).

    Why one class instead of a Strategy split like satellite/radar: the read
    path is identical across modes — only the write side differs by a few
    branches, so the boolean gates are cleaner than duplicating the reader.
    """

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: Optional[S3Client],
        http_client: HttpTileClient,
        providers: dict[str, BasemapProvider],
        tile_ttl: int,
        cache_concurrent: int,
        online_fallback: bool,
        negative_cache_enabled: bool = True,
        negative_cache_ttl: int = 300,
        request_deadline_seconds: float = 4.0,
        redis_cache_enabled: bool = True,
        s3_cache_enabled: bool = True,
    ):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self._redis = redis_client
        self._s3 = s3_client
        self._http = http_client
        self._providers = providers
        self._tile_ttl = tile_ttl
        self._online_fallback = online_fallback
        self._redis_cache_enabled = redis_cache_enabled
        # An s3_client of None is only valid if S3 is off in both read and
        # write directions (relay_only mode). Enforce the invariant here.
        self._s3_cache_enabled = s3_cache_enabled and s3_client is not None
        if s3_cache_enabled and s3_client is None:
            raise ValueError(
                "BasemapTileReader: s3_cache_enabled=True requires an "
                "s3_client; got None."
            )
        # Tombstones are Redis writes; without the Redis cache they make no
        # sense. Force-disable so callers can't accidentally leave it on.
        self._negative_cache_enabled = negative_cache_enabled and redis_cache_enabled
        self._negative_cache_ttl = negative_cache_ttl
        self._request_deadline_seconds = request_deadline_seconds
        self._cache_semaphore = asyncio.Semaphore(cache_concurrent)
        self._inflight_cache_tasks: set[asyncio.Task] = set()
        self._inflight: dict[_TileKey, "asyncio.Future[Optional[bytes]]"] = {}
        logger.info(
            "BasemapTileReader redis=%s s3=%s online_fallback=%s "
            "negative_cache=%s ttl=%ds deadline=%.1fs",
            "enabled" if redis_cache_enabled else "disabled",
            "enabled" if s3_cache_enabled else "disabled",
            online_fallback,
            "enabled" if self._negative_cache_enabled else "disabled",
            negative_cache_ttl,
            request_deadline_seconds,
        )

    async def get_tile(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Fetch a tile with Redis → S3 → (optional) external-provider fallback."""
        if self._redis_cache_enabled:
            data = await self._redis.get_basemap_tile(provider_id, z, x, y)
            if data:
                self._log_served("redis", provider_id, z, x, y)
                return data

        key: _TileKey = (provider_id, z, x, y)
        existing = self._inflight.get(key)
        if existing is not None:
            return await existing

        fut: "asyncio.Future[Optional[bytes]]" = (
            asyncio.get_event_loop().create_future()
        )
        self._inflight[key] = fut
        try:
            try:
                result = await asyncio.wait_for(
                    self._fetch_cold(provider_id, z, x, y),
                    timeout=self._request_deadline_seconds,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Tile request deadline exceeded: %s/%d/%d/%d (%.1fs)",
                    provider_id,
                    z,
                    x,
                    y,
                    self._request_deadline_seconds,
                )
                self._schedule_cache_write(
                    self._mark_miss(provider_id, z, x, y),
                    label=f"deadline-miss {provider_id}/{z}/{x}/{y}",
                )
                result = None
            if not fut.done():
                fut.set_result(result)
            return result
        except BaseException as exc:  # pylint: disable=broad-exception-caught
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            self._inflight.pop(key, None)

    async def _fetch_cold(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Cold-path lookup: negative cache + S3, then relay, then tombstone."""
        s3_key = S3Client.build_basemap_tile_key(provider_id, z, x, y)

        miss_flag, s3_data = await self._parallel_miss_and_s3(
            provider_id, z, x, y, s3_key
        )

        if s3_data:
            if self._redis_cache_enabled:
                self._schedule_cache_write(
                    self._redis.store_basemap_tile(
                        provider_id, z, x, y, s3_data, ttl=self._tile_ttl
                    ),
                    label=f"redis-after-s3 {s3_key}",
                )
            self._log_served("s3", provider_id, z, x, y)
            return s3_data

        if miss_flag:
            return None

        provider = self._providers.get(provider_id)
        if not self._online_fallback or not provider:
            await self._mark_miss(provider_id, z, x, y)
            return None

        url = build_source_url(provider, z, x, y)
        try:
            data = await self._http.download_tile(url)
        except ProviderUnavailableError as exc:
            # Upstream provider is unreachable — degrade to a miss rather than
            # surfacing the error to the user. The scraper's circuit breaker
            # handles the health signal; the reader's job is just to get
            # bytes or return None.
            logger.info(
                "Relay unavailable for %s/%d/%d/%d: %s", provider_id, z, x, y, exc.cause
            )
            await self._mark_miss(provider_id, z, x, y)
            return None
        if data:
            if self._s3_cache_enabled or self._redis_cache_enabled:
                self._schedule_cache_write(
                    self._cache_tile(provider_id, z, x, y, s3_key, data),
                    label=f"s3+redis-after-provider {s3_key}",
                )
            if self._negative_cache_enabled:
                self._schedule_cache_write(
                    self._redis.clear_basemap_tile_miss(provider_id, z, x, y),
                    label=f"clear-miss-after-provider {s3_key}",
                )
            self._log_served("relay", provider_id, z, x, y)
            return data

        await self._mark_miss(provider_id, z, x, y)
        return None

    async def _parallel_miss_and_s3(
        self, provider_id: str, z: int, x: int, y: int, s3_key: str
    ) -> Tuple[bool, Optional[bytes]]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Fail-open pipeline: run the neg-cache check and S3 GET concurrently."""
        if not self._s3_cache_enabled and not self._negative_cache_enabled:
            # relay_only: skip both probes, go straight to the provider.
            return False, None
        if not self._s3_cache_enabled:
            miss = await self._redis.get_basemap_tile_miss(provider_id, z, x, y)
            return bool(miss), None
        assert self._s3 is not None  # guaranteed by _s3_cache_enabled
        if not self._negative_cache_enabled:
            return False, await self._s3.download_tile(s3_key)

        miss_res, s3_res = await asyncio.gather(
            self._redis.get_basemap_tile_miss(provider_id, z, x, y),
            self._s3.download_tile(s3_key),
            return_exceptions=True,
        )
        if isinstance(miss_res, BaseException):
            logger.debug("Negative-cache check failed, degrading: %s", miss_res)
            miss_flag = False
        else:
            miss_flag = bool(miss_res)
        if isinstance(s3_res, BaseException):
            logger.warning("S3 lookup raised for %s: %s", s3_key, s3_res)
            s3_data: Optional[bytes] = None
        else:
            s3_data = s3_res
        return miss_flag, s3_data

    @staticmethod
    def _log_served(source: str, provider_id: str, z: int, x: int, y: int) -> None:
        """Emit the tier a successfully-served tile came from."""
        logger.info(
            "Served basemap tile from %s: %s/%d/%d/%d",
            source,
            provider_id,
            z,
            x,
            y,
        )

    async def _mark_miss(self, provider_id: str, z: int, x: int, y: int) -> None:
        """Best-effort negative-cache write. Never fails the request."""
        if not self._negative_cache_enabled:
            return
        try:
            await self._redis.mark_basemap_tile_miss(
                provider_id, z, x, y, ttl=self._negative_cache_ttl
            )
        except (httpx.HTTPError, asyncio.TimeoutError, OSError) as exc:
            logger.debug(
                "Failed to mark tile miss %s/%d/%d/%d: %s", provider_id, z, x, y, exc
            )

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
        """Persist a freshly-fetched tile into S3 and Redis (both gated)."""
        if self._s3_cache_enabled:
            assert self._s3 is not None  # guaranteed by _s3_cache_enabled
            await self._s3.upload_tile(s3_key, data)
        if self._redis_cache_enabled:
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
