"""Sync strategies for radar tile retrieval."""

import asyncio
import json
import re
from typing import List, Optional, Protocol

from clients.redis_client import RedisClient
from clients.s3_client import S3Client

# pylint: disable=too-many-arguments,too-many-positional-arguments

# S3 prefix where radar data lives
RADAR_S3_PREFIX = "radar"


class RadarSyncStrategy(Protocol):
    """Protocol for radar sync strategies."""

    async def get_tile(
        self,
        radar_id: str,
        variable_id: str,
        elevation_id: str,
        tileset_id: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        """Get tile data for the given radar coordinates."""

    async def list_radars(self) -> List[str]:
        """List all available radar IDs."""

    async def list_variables(self, radar_id: str) -> List[str]:
        """List all variable IDs for a radar."""

    async def list_elevations(self, radar_id: str, variable_id: str) -> List[str]:
        """List all elevation IDs for a radar/variable."""

    async def list_tilesets(
        self, radar_id: str, variable_id: str, elevation_id: str
    ) -> List[str]:
        """List all tileset IDs for a radar/variable/elevation."""


class RadarFullSyncStrategy:
    """Reads from pre-populated Redis (background sync fills it)."""

    def __init__(self, redis_client: RedisClient):
        self._redis = redis_client

    async def get_tile(
        self,
        radar_id: str,
        variable_id: str,
        elevation_id: str,
        tileset_id: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        """Get radar tile from Redis."""
        return await self._redis.get_radar_tile(
            radar_id, variable_id, tileset_id, elevation_id, z, x, y
        )

    async def list_radars(self) -> List[str]:
        """List radars from Redis index."""
        return await self._redis.get_radar_radars()

    async def list_variables(self, radar_id: str) -> List[str]:
        """List variables from Redis index."""
        return await self._redis.get_radar_variables(radar_id)

    async def list_elevations(self, radar_id: str, variable_id: str) -> List[str]:
        """List elevations from Redis index."""
        return await self._redis.get_radar_elevations(radar_id, variable_id)

    async def list_tilesets(
        self, radar_id: str, variable_id: str, elevation_id: str
    ) -> List[str]:
        """List tilesets from Redis index."""
        return await self._redis.get_radar_tilesets(radar_id, variable_id, elevation_id)


class RadarOnDemandStrategy:
    """Tries Redis first, falls back to S3, caches with TTL."""

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: Optional[S3Client],
        tile_ttl: int,
        listing_ttl: int,
    ):
        self._redis = redis_client
        self._s3 = s3_client
        self._tile_ttl = tile_ttl
        self._listing_ttl = listing_ttl

    async def get_tile(
        self,
        radar_id: str,
        variable_id: str,
        elevation_id: str,
        tileset_id: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        """Get tile from Redis, falling back to S3 with cache-aside."""
        data = await self._redis.get_radar_tile(
            radar_id, variable_id, tileset_id, elevation_id, z, x, y
        )
        if data:
            return data

        if not self._s3:
            return None

        s3_key = S3Client.build_radar_tile_key(
            radar_id, variable_id, tileset_id, elevation_id, z, x, y
        )
        data = await self._s3.download_tile(s3_key)
        if data:
            asyncio.create_task(
                self._redis.store_radar_tile(
                    radar_id,
                    variable_id,
                    tileset_id,
                    elevation_id,
                    z,
                    x,
                    y,
                    data,
                    ttl=self._tile_ttl,
                )
            )
            return data

        return None

    async def list_radars(self) -> List[str]:
        """List radars from cache or S3."""
        cache_key = "cache:listing:radar:radars"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        subdirs = await self._s3.get_subdirectories(RADAR_S3_PREFIX)
        radars = sorted(s.rstrip("/").split("/")[-1] for s in subdirs)

        await self._redis.cache_listing(
            cache_key, json.dumps(radars).encode(), self._listing_ttl
        )
        return radars

    async def list_variables(self, radar_id: str) -> List[str]:
        """List variables from cache or S3."""
        cache_key = f"cache:listing:radar:{radar_id}:variables"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        prefix = f"{RADAR_S3_PREFIX}/{radar_id}"
        subdirs = await self._s3.get_subdirectories(prefix)
        variables = sorted(s.rstrip("/").split("/")[-1] for s in subdirs)

        await self._redis.cache_listing(
            cache_key, json.dumps(variables).encode(), self._listing_ttl
        )
        return variables

    async def list_elevations(self, radar_id: str, variable_id: str) -> List[str]:
        """List elevations from cache or S3."""
        cache_key = f"cache:listing:radar:{radar_id}:{variable_id}:elevations"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        prefix = f"{RADAR_S3_PREFIX}/{radar_id}/{variable_id}"
        subdirs = await self._s3.get_subdirectories(prefix)
        elevations = set()
        for subdir in subdirs:
            name = subdir.rstrip("/").split("/")[-1]
            match = re.search(r"_(elev\d+)$", name)
            if match:
                elevations.add(match.group(1))
        result = sorted(elevations)

        await self._redis.cache_listing(
            cache_key, json.dumps(result).encode(), self._listing_ttl
        )
        return result

    async def list_tilesets(
        self, radar_id: str, variable_id: str, elevation_id: str
    ) -> List[str]:
        """List tilesets from cache or S3."""
        cache_key = (
            f"cache:listing:radar:{radar_id}:{variable_id}:{elevation_id}:tilesets"
        )
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        prefix = f"{RADAR_S3_PREFIX}/{radar_id}/{variable_id}"
        subdirs = await self._s3.get_subdirectories(prefix)
        tilesets = []
        suffix = f"_{elevation_id}"
        for subdir in subdirs:
            name = subdir.rstrip("/").split("/")[-1]
            if name.endswith(suffix):
                tileset_id = name[: -len(suffix)]
                tilesets.append(tileset_id)
        tilesets.sort(reverse=True)

        await self._redis.cache_listing(
            cache_key, json.dumps(tilesets).encode(), self._listing_ttl
        )
        return tilesets
