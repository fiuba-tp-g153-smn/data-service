"""Read strategies for GFS tiles, overlays and listings.

Unlike the other model domains, GFS raster tiles are **never** pre-synced The
background loop mirrors only listings and the single-file overlays; tiles and
barb tiles are read straight from S3 and cached lazily on first hit.
"""

import asyncio
import json
from typing import List, Optional, Protocol

from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.gfs_config import get_product, leaf_segment, step_from_basename


class GfsSyncStrategy(Protocol):
    """Protocol for GFS read strategies."""

    async def get_tile(
        self, product_id: str, cycle: str, fxxx: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get a raster tile for the given coordinates."""

    async def get_geojson(
        self, product_id: str, cycle: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        """Get a single-file overlay (isobars, thickness, heights, isotherms)."""

    async def get_barb_tile(
        self, product_id: str, cycle: str, fxxx: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get one wind-barb GeoJSON tile."""

    async def list_cycles(self, product_id: str) -> List[str]:
        """List available cycles, newest first."""

    async def list_steps(self, product_id: str, cycle: str) -> List[str]:
        """List forecast steps of a cycle, ascending."""

    async def list_layers(self, product_id: str, cycle: str, fxxx: str) -> List[str]:
        """List the overlay layers a step actually has, sorted."""


def _s3_segment(product_id: str) -> Optional[str]:
    """S3 path segment for a product id, or None when the id is unknown."""
    product = get_product(product_id)
    return product.s3_segment if product else None


class GfsOnDemandStrategy:
    """Reads from S3, caching each hit in Redis with a TTL.

    This is the whole read path for tiles and barbs, and the miss path for
    everything else.
    """

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
        self, product_id: str, cycle: str, fxxx: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Serve a raster tile, caching the S3 read for subsequent viewers."""
        data = await self._redis.get_gfs_tile(product_id, cycle, fxxx, z, x, y)
        if data:
            return data

        segment = _s3_segment(product_id)
        if not self._s3 or segment is None:
            return None

        s3_key = S3Client.build_gfs_tile_key(segment, cycle, fxxx, z, x, y)
        data = await self._s3.download_tile(s3_key)
        if data:
            asyncio.create_task(
                self._redis.store_gfs_tile(
                    product_id, cycle, fxxx, z, x, y, data, ttl=self._tile_ttl
                )
            )
        return data

    async def get_geojson(
        self, product_id: str, cycle: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        """Serve a single-file overlay, caching the S3 read."""
        data = await self._redis.get_gfs_geojson(product_id, cycle, fxxx, layer)
        if data:
            return data

        segment = _s3_segment(product_id)
        if not self._s3 or segment is None:
            return None

        s3_key = S3Client.build_gfs_geojson_key(segment, cycle, fxxx, layer)
        data = await self._s3.download_tile(s3_key)
        if data:
            asyncio.create_task(
                self._redis.store_gfs_geojson(
                    product_id, cycle, fxxx, layer, data, ttl=self._geojson_ttl
                )
            )
        return data

    async def get_barb_tile(
        self, product_id: str, cycle: str, fxxx: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Barbs go straight to S3 and are not cached.

        There are as many barb objects as tiles, and each is small; caching them
        would fill Redis with single-use keys for a layer the viewport already
        fetches sparsely.
        """
        segment = _s3_segment(product_id)
        if not self._s3 or segment is None:
            return None
        s3_key = S3Client.build_gfs_barb_tile_key(segment, cycle, fxxx, z, x, y)
        return await self._s3.download_tile(s3_key)

    async def list_cycles(self, product_id: str) -> List[str]:
        """Discover cycles from the COG prefix.

        Deliberately not the tiles prefix: `mslp` is contour-only and produces
        no raster, so listing tiles would make that product look empty.
        """
        cache_key = f"cache:listing:gfs:{product_id}:cycles"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        segment = _s3_segment(product_id)
        if not self._s3 or segment is None:
            return []

        subdirs = await self._s3.get_subdirectories(
            S3Client.gfs_cog_cycle_prefix(segment)
        )
        cycles = sorted(
            (name for name in (leaf_segment(s) for s in subdirs) if name), reverse=True
        )

        await self._redis.cache_listing(
            cache_key, json.dumps(cycles).encode(), self._listing_ttl
        )
        return cycles

    async def list_steps(self, product_id: str, cycle: str) -> List[str]:
        """Recover steps from the COG basenames of a cycle.

        tiles-processor names them `{cycle}_{fxxx}.tif`, so the cycle prefix is
        stripped back off to leave the bare step.
        """
        cache_key = f"cache:listing:gfs:{product_id}:{cycle}:steps"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        segment = _s3_segment(product_id)
        if not self._s3 or segment is None:
            return []

        basenames = await self._s3.list_object_basenames(
            f"{S3Client.gfs_cog_cycle_prefix(segment)}{cycle}/", ".tif", delimiter="/"
        )
        steps = sorted(
            step
            for step in (step_from_basename(name, cycle) for name in basenames)
            if step
        )

        await self._redis.cache_listing(
            cache_key, json.dumps(steps).encode(), self._listing_ttl
        )
        return steps

    async def list_layers(self, product_id: str, cycle: str, fxxx: str) -> List[str]:
        # `cycle` and `fxxx` are unused: they belong to the Protocol signature,
        # which the Redis-backed strategy does need.
        # pylint: disable=unused-argument
        """Layers come from the catalogue, not from a per-step S3 listing.

        Which overlays a product carries is fixed by the processor that writes
        them, so probing S3 for every step would spend one LIST per step to
        rediscover a constant.
        """
        product = get_product(product_id)
        if product is None:
            return []
        layers = list(product.layers)
        if product.has_barbs:
            layers.append("barbs")
        return sorted(layers)


class GfsFullSyncStrategy:
    """Redis-first for what the background loop mirrors, S3 for the rest.

    Listings and single-file overlays are pre-warmed by `GfsSyncService`, so
    they are read from Redis and only fall back to S3 when the index is cold or
    evicted. Tiles and barbs are delegated to the on-demand path unconditionally
    — they are never pre-synced.
    """

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: Optional[S3Client] = None,
        tile_ttl: int = 0,
        geojson_ttl: int = 0,
        listing_ttl: int = 0,
    ):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self._redis = redis_client
        self._fallback = GfsOnDemandStrategy(
            redis_client, s3_client, tile_ttl, geojson_ttl, listing_ttl
        )

    async def get_tile(
        self, product_id: str, cycle: str, fxxx: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Straight to the on-demand path: tiles are never pre-synced."""
        return await self._fallback.get_tile(product_id, cycle, fxxx, z, x, y)

    async def get_geojson(
        self, product_id: str, cycle: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        """Pre-warmed by the sync loop, so Redis first."""
        data = await self._redis.get_gfs_geojson(product_id, cycle, fxxx, layer)
        if data:
            return data
        return await self._fallback.get_geojson(product_id, cycle, fxxx, layer)

    async def get_barb_tile(
        self, product_id: str, cycle: str, fxxx: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Straight to the on-demand path: barb tiles are never pre-synced."""
        return await self._fallback.get_barb_tile(product_id, cycle, fxxx, z, x, y)

    async def list_cycles(self, product_id: str) -> List[str]:
        """Redis index first, S3 when it is cold or evicted."""
        cycles = await self._redis.get_gfs_cycles(product_id)
        if cycles:
            return cycles
        return await self._fallback.list_cycles(product_id)

    async def list_steps(self, product_id: str, cycle: str) -> List[str]:
        """Redis index first, S3 when it is cold or evicted."""
        steps = await self._redis.get_gfs_steps(product_id, cycle)
        if steps:
            return steps
        return await self._fallback.list_steps(product_id, cycle)

    async def list_layers(self, product_id: str, cycle: str, fxxx: str) -> List[str]:
        """Redis index first, then the catalogue via the on-demand path."""
        layers = await self._redis.get_gfs_layers(product_id, cycle, fxxx)
        if layers:
            return layers
        return await self._fallback.list_layers(product_id, cycle, fxxx)
