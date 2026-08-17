"""Sync strategies for WRF tile + GeoJSON retrieval."""

import asyncio
import json
from typing import List, Optional, Protocol

from clients.redis_client import RedisClient
from clients.s3_client import S3Client

WRF_S3_PREFIX = "tiles/wrf"


class WrfSyncStrategy(Protocol):
    """Protocol for WRF sync strategies."""

    async def get_tile(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        """Get tile data for the given WRF coordinates."""

    async def get_geojson(
        self, product_id: str, init_tag: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        """Get a GeoJSON layer (barbs / contours) for the given step."""

    async def get_barb_tile(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        """Get a rasterized WRF wind-barb WebP tile for (z, x, y)."""

    async def list_init_runs(self, product_id: str) -> List[str]:
        """List available initialization run tags, sorted descending."""

    async def list_steps(self, product_id: str, init_tag: str) -> List[str]:
        """List forecast steps for a product/init_tag, sorted ascending."""

    async def list_layers(self, product_id: str, init_tag: str, fxxx: str) -> List[str]:
        """List GeoJSON layers available for a step, sorted ascending."""


class WrfFullSyncStrategy:
    """Redis-first reads (background sync pre-warms), with an S3 fallback.

    Tiles, GeoJSON layers, and listings fall back to S3 when Redis misses / its
    index is empty (evicted or cold), delegating the S3 miss-path to
    ``WrfOnDemandStrategy``. Barb tiles are never pre-synced (too many files) so
    they always go direct to S3. Without an S3 client the behaviour is
    Redis-only (unchanged) and barb tiles return None.
    """

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: Optional[S3Client] = None,
        tile_ttl: int = 0,
        geojson_ttl: int = 0,
        listing_ttl: int = 0,
        inits_to_keep: int = 0,
    ):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self._redis = redis_client
        self._s3 = s3_client
        self._fallback = (
            WrfOnDemandStrategy(
                redis_client,
                s3_client,
                tile_ttl,
                geojson_ttl,
                listing_ttl,
                inits_to_keep,
            )
            if s3_client is not None
            else None
        )

    async def get_tile(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        data = await self._redis.get_wrf_tile(product_id, init_tag, fxxx, z, x, y)
        if data:
            return data
        if self._fallback is not None:
            return await self._fallback.get_tile(product_id, init_tag, fxxx, z, x, y)
        return None

    async def get_geojson(
        self, product_id: str, init_tag: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        data = await self._redis.get_wrf_geojson(product_id, init_tag, fxxx, layer)
        if data:
            return data
        if self._fallback is not None:
            return await self._fallback.get_geojson(product_id, init_tag, fxxx, layer)
        return None

    async def get_barb_tile(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        if not self._s3:
            return None
        s3_key = S3Client.build_wrf_barb_tile_key(product_id, init_tag, fxxx, z, x, y)
        return await self._s3.download_tile(s3_key)

    async def list_init_runs(self, product_id: str) -> List[str]:
        init_runs = await self._redis.get_wrf_init_runs(product_id)
        if init_runs:
            return init_runs
        if self._fallback is not None:
            return await self._fallback.list_init_runs(product_id)
        return []

    async def list_steps(self, product_id: str, init_tag: str) -> List[str]:
        steps = await self._redis.get_wrf_steps(product_id, init_tag)
        if steps:
            return steps
        if self._fallback is not None:
            return await self._fallback.list_steps(product_id, init_tag)
        return []

    async def list_layers(self, product_id: str, init_tag: str, fxxx: str) -> List[str]:
        layers = await self._redis.get_wrf_layers(product_id, init_tag, fxxx)
        if layers:
            return layers
        if self._fallback is not None:
            return await self._fallback.list_layers(product_id, init_tag, fxxx)
        return []


class WrfOnDemandStrategy:
    """Tries Redis first, falls back to S3, caches with TTL."""

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: Optional[S3Client],
        tile_ttl: int,
        geojson_ttl: int,
        listing_ttl: int,
        inits_to_keep: int = 0,
    ):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self._redis = redis_client
        self._s3 = s3_client
        self._tile_ttl = tile_ttl
        self._geojson_ttl = geojson_ttl
        self._listing_ttl = listing_ttl
        self._inits_to_keep = inits_to_keep

    async def get_tile(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        data = await self._redis.get_wrf_tile(product_id, init_tag, fxxx, z, x, y)
        if data:
            return data

        if not self._s3:
            return None

        s3_key = S3Client.build_wrf_tile_key(product_id, init_tag, fxxx, z, x, y)
        data = await self._s3.download_tile(s3_key)
        if data:
            asyncio.create_task(
                self._redis.store_wrf_tile(
                    product_id, init_tag, fxxx, z, x, y, data, ttl=self._tile_ttl
                )
            )
        return data

    async def get_geojson(
        self, product_id: str, init_tag: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        data = await self._redis.get_wrf_geojson(product_id, init_tag, fxxx, layer)
        if data:
            return data

        if not self._s3:
            return None

        s3_key = S3Client.build_wrf_geojson_key(product_id, init_tag, fxxx, layer)
        data = await self._s3.download_tile(s3_key)
        if data:
            asyncio.create_task(
                self._redis.store_wrf_geojson(
                    product_id, init_tag, fxxx, layer, data, ttl=self._geojson_ttl
                )
            )
        return data

    async def get_barb_tile(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        if not self._s3:
            return None
        s3_key = S3Client.build_wrf_barb_tile_key(product_id, init_tag, fxxx, z, x, y)
        return await self._s3.download_tile(s3_key)

    async def list_init_runs(self, product_id: str) -> List[str]:
        cache_key = f"cache:listing:wrf:{product_id}:init_runs"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        subdirs = await self._s3.try_get_subdirectories(f"{WRF_S3_PREFIX}/{product_id}")
        init_runs = sorted(
            (
                s.rstrip("/").split("/")[-1]
                for s in subdirs
                if s.rstrip("/").split("/")[-1]
            ),
            reverse=True,
        )
        if self._inits_to_keep > 0:
            init_runs = init_runs[: self._inits_to_keep]

        await self._redis.cache_listing(
            cache_key, json.dumps(init_runs).encode(), self._listing_ttl
        )
        return init_runs

    async def list_steps(self, product_id: str, init_tag: str) -> List[str]:
        cache_key = f"cache:listing:wrf:{product_id}:{init_tag}:steps"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        subdirs = await self._s3.try_get_subdirectories(
            f"{WRF_S3_PREFIX}/{product_id}/{init_tag}"
        )
        steps = sorted(
            s.rstrip("/").split("/")[-1]
            for s in subdirs
            if s.rstrip("/").split("/")[-1]
        )

        await self._redis.cache_listing(
            cache_key, json.dumps(steps).encode(), self._listing_ttl
        )
        return steps

    async def list_layers(self, product_id: str, init_tag: str, fxxx: str) -> List[str]:
        cache_key = f"cache:listing:wrf:{product_id}:{init_tag}:{fxxx}:layers"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        layers = sorted(await self._s3.list_wrf_layers(product_id, init_tag, fxxx))
        await self._redis.cache_listing(
            cache_key, json.dumps(layers).encode(), self._listing_ttl
        )
        return layers
