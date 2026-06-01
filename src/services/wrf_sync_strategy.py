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

    async def list_layers(
        self, product_id: str, init_tag: str, fxxx: str
    ) -> List[str]:
        """List GeoJSON layers available for a step, sorted ascending."""


class WrfFullSyncStrategy:
    """Reads from pre-populated Redis (background sync fills it).

    Barb tiles are not pre-synced (too many files); they go direct to S3
    via the optional S3 client passed at construction time.
    """

    def __init__(
        self, redis_client: RedisClient, s3_client: Optional[S3Client] = None
    ):
        self._redis = redis_client
        self._s3 = s3_client

    async def get_tile(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        return await self._redis.get_wrf_tile(product_id, init_tag, fxxx, z, x, y)

    async def get_geojson(
        self, product_id: str, init_tag: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        return await self._redis.get_wrf_geojson(product_id, init_tag, fxxx, layer)

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
        return await self._redis.get_wrf_init_runs(product_id)

    async def list_steps(self, product_id: str, init_tag: str) -> List[str]:
        return await self._redis.get_wrf_steps(product_id, init_tag)

    async def list_layers(
        self, product_id: str, init_tag: str, fxxx: str
    ) -> List[str]:
        return await self._redis.get_wrf_layers(product_id, init_tag, fxxx)


class WrfOnDemandStrategy:
    """Tries Redis first, falls back to S3, caches with TTL."""

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: Optional[S3Client],
        tile_ttl: int,
        geojson_ttl: int,
        listing_ttl: int,
    ):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self._redis = redis_client
        self._s3 = s3_client
        self._tile_ttl = tile_ttl
        self._geojson_ttl = geojson_ttl
        self._listing_ttl = listing_ttl

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

        subdirs = await self._s3.get_subdirectories(
            f"{WRF_S3_PREFIX}/{product_id}"
        )
        init_runs = sorted(
            (
                s.rstrip("/").split("/")[-1]
                for s in subdirs
                if s.rstrip("/").split("/")[-1]
            ),
            reverse=True,
        )

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

        subdirs = await self._s3.get_subdirectories(
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

    async def list_layers(
        self, product_id: str, init_tag: str, fxxx: str
    ) -> List[str]:
        cache_key = f"cache:listing:wrf:{product_id}:{init_tag}:{fxxx}:layers"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        layers = sorted(
            await self._s3.list_wrf_layers(product_id, init_tag, fxxx)
        )
        await self._redis.cache_listing(
            cache_key, json.dumps(layers).encode(), self._listing_ttl
        )
        return layers
