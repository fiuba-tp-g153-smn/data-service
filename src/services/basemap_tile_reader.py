"""Base map tile reader with prod-first fallback (provider → Redis → S3)."""

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
    Read a basemap tile using a prod-first fallback chain:

      Tier 1 — External provider (upstream is authoritative; only HTTP
               errors / unreachable count as failure).
      Tier 2 — Redis hot cache (recent successful fetches).
      Tier 3 — SeaweedFS S3 cold backup (populated by the scraper).

    Rationale: upstream is the canonical truth, so each request tries it
    first. Redis and S3 only get consulted when the upstream call fails.
    Cache write-throughs are scheduled as background tasks (throttled by
    `cache_semaphore`) so they never block the request path.

    Single-flight in-flight dedup keyed on `(provider_id, z, x, y)` collapses
    bursts of concurrent requests for the same tile down to one upstream
    round-trip. The whole chain runs under a wall-clock deadline.

    Mode behaviour is driven by two boolean knobs configured by
    `main.configure_basemap`:
      * `redis_cache_enabled=False` skips the Redis tier (read + write).
        Used by `basemap_sync_mode = "no_cache"` and `"relay_only"`.
      * `s3_cache_enabled=False` skips the S3 tier (read + write). Used by
        `"relay_only"` so the reader becomes a pure provider proxy.

    `online_fallback=False` disables the upstream tier entirely — reads
    degrade to Redis → S3 only (the legacy offline-read path).
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
        request_deadline_seconds: float = 4.0,
        redis_cache_enabled: bool = True,
        s3_cache_enabled: bool = True,
        negative_cache_enabled: bool = True,
        negative_cache_ttl: int = 300,
    ):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self._redis = redis_client
        self._s3 = s3_client
        self._http = http_client
        self._providers = providers
        self._tile_ttl = tile_ttl
        self._online_fallback = online_fallback
        self._redis_cache_enabled = redis_cache_enabled
        # The miss tombstone lives in Redis, so it can only work when the Redis
        # tier is enabled. Short-circuits repeat requests for a tile that missed
        # the whole chain, sparing upstream a relay per request.
        self._negative_cache_enabled = negative_cache_enabled and redis_cache_enabled
        self._negative_cache_ttl = negative_cache_ttl
        # An s3_client of None is only valid if S3 is off in both read and
        # write directions (relay_only mode). Enforce the invariant here.
        self._s3_cache_enabled = s3_cache_enabled and s3_client is not None
        if s3_cache_enabled and s3_client is None:
            raise ValueError(
                "BasemapTileReader: s3_cache_enabled=True requires an "
                "s3_client; got None."
            )
        self._request_deadline_seconds = request_deadline_seconds
        self._cache_semaphore = asyncio.Semaphore(cache_concurrent)
        self._inflight_cache_tasks: set[asyncio.Task] = set()
        self._inflight: dict[_TileKey, "asyncio.Future[Optional[bytes]]"] = {}
        logger.info(
            "BasemapTileReader (prod-first) redis=%s s3=%s online_fallback=%s "
            "deadline=%.1fs",
            "enabled" if redis_cache_enabled else "disabled",
            "enabled" if s3_cache_enabled else "disabled",
            online_fallback,
            request_deadline_seconds,
        )

    async def get_tile(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Fetch a tile with provider → Redis → S3 fallback under single-flight."""
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
                    self._resolve_tile(provider_id, z, x, y),
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

    async def _resolve_tile(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Run the prod → Redis → S3 chain; return the first hit or None."""
        s3_key = S3Client.build_basemap_tile_key(provider_id, z, x, y)

        # Tier 0 — negative cache. A recent full-chain miss short-circuits here
        # so a tile that legitimately doesn't exist upstream isn't relayed on
        # every repeat request (empty-ocean / above-coverage panning).
        if self._negative_cache_enabled and await self._safe_get_miss(
            provider_id, z, x, y
        ):
            self._log_served("miss-cache", provider_id, z, x, y)
            return None

        # Tier 1 — external provider (upstream). Only HTTP errors count as
        # failure; a 200 with bytes is success even if visually empty.
        prod_data = await self._try_provider(provider_id, z, x, y)
        if prod_data is not None:
            if self._redis_cache_enabled:
                self._schedule_cache_write(
                    self._write_redis(provider_id, z, x, y, prod_data),
                    label=f"redis-after-provider {s3_key}",
                )
            if self._s3_cache_enabled:
                self._schedule_cache_write(
                    self._write_s3(s3_key, prod_data),
                    label=f"s3-after-provider {s3_key}",
                )
            self._log_served("relay", provider_id, z, x, y)
            return prod_data

        # Tier 2 — Redis hot cache.
        if self._redis_cache_enabled:
            redis_data = await self._safe_redis_get(provider_id, z, x, y)
            if redis_data:
                self._log_served("redis", provider_id, z, x, y)
                return redis_data

        # Tier 3 — S3 cold backup. Write back to Redis on hit so the next
        # request can short-circuit at tier 2 if upstream is still down.
        if self._s3_cache_enabled:
            assert self._s3 is not None  # guaranteed by _s3_cache_enabled
            s3_data = await self._safe_s3_get(s3_key)
            if s3_data:
                if self._redis_cache_enabled:
                    self._schedule_cache_write(
                        self._write_redis(provider_id, z, x, y, s3_data),
                        label=f"redis-after-s3 {s3_key}",
                    )
                self._log_served("s3", provider_id, z, x, y)
                return s3_data

        # Confirmed full-chain miss — tombstone it so repeats short-circuit at
        # tier 0 until the TTL lapses or the scraper stores the tile.
        if self._negative_cache_enabled:
            self._schedule_cache_write(
                self._redis.mark_basemap_tile_miss(
                    provider_id, z, x, y, self._negative_cache_ttl
                ),
                label=f"miss-mark {s3_key}",
            )
        return None

    async def _try_provider(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Fetch from upstream. Returns bytes on success, None on any failure."""
        if not self._online_fallback:
            return None
        provider = self._providers.get(provider_id)
        if not provider:
            return None
        url = build_source_url(provider, z, x, y)
        try:
            return await self._http.download_tile(url)
        except ProviderUnavailableError as exc:
            # Upstream unreachable — degrade to caches. The scraper's circuit
            # breaker owns the health signal; the reader's job is just bytes.
            logger.info(
                "Relay unavailable for %s/%d/%d/%d: %s",
                provider_id,
                z,
                x,
                y,
                exc.cause,
            )
            return None

    async def _safe_redis_get(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Redis GET that swallows transport errors and returns None."""
        try:
            return await self._redis.get_basemap_tile(provider_id, z, x, y)
        except (httpx.HTTPError, asyncio.TimeoutError, OSError) as exc:
            logger.warning(
                "Redis lookup failed for %s/%d/%d/%d: %s", provider_id, z, x, y, exc
            )
            return None

    async def _safe_get_miss(self, provider_id: str, z: int, x: int, y: int) -> bool:
        """Check the miss tombstone, swallowing transport errors as 'no tombstone'.

        A Redis hiccup must never turn into a short-circuit that hides a tile —
        on any error we fall through to the normal provider/S3 chain.
        """
        try:
            return await self._redis.get_basemap_tile_miss(provider_id, z, x, y)
        except (httpx.HTTPError, asyncio.TimeoutError, OSError) as exc:
            logger.warning(
                "Miss-cache lookup failed for %s/%d/%d/%d: %s",
                provider_id,
                z,
                x,
                y,
                exc,
            )
            return False

    async def _safe_s3_get(self, s3_key: str) -> Optional[bytes]:
        """S3 GET that swallows transport errors and returns None."""
        assert self._s3 is not None
        try:
            return await self._s3.download_tile(s3_key)
        except (httpx.HTTPError, asyncio.TimeoutError, OSError) as exc:
            logger.warning("S3 lookup failed for %s: %s", s3_key, exc)
            return None

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

    async def _write_redis(
        self, provider_id: str, z: int, x: int, y: int, data: bytes
    ) -> None:
        """Persist a tile in Redis with the configured TTL."""
        await self._redis.store_basemap_tile(
            provider_id, z, x, y, data, ttl=self._tile_ttl
        )

    async def _write_s3(self, s3_key: str, data: bytes) -> None:
        """Persist a tile in S3 under the canonical basemap key."""
        assert self._s3 is not None
        await self._s3.upload_tile(s3_key, data)

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
