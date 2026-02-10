"""Sync strategies for radar tile retrieval."""

import asyncio
import json
import re
from pathlib import Path
from typing import List, Optional, Protocol

from clients.redis_client import RedisClient

# pylint: disable=too-many-arguments,too-many-positional-arguments


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
    """Tries Redis first, falls back to filesystem, caches with TTL."""

    def __init__(
        self,
        redis_client: RedisClient,
        output_path: Path,
        tile_ttl: int,
        listing_ttl: int,
    ):
        self._redis = redis_client
        self._output_path = output_path
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
        """Get tile from Redis, falling back to filesystem with cache-aside."""
        # Always try Redis first
        data = await self._redis.get_radar_tile(
            radar_id, variable_id, tileset_id, elevation_id, z, x, y
        )
        if data:
            return data

        # Fall back to filesystem
        tile_path = (
            self._output_path
            / radar_id
            / variable_id
            / f"{tileset_id}_{elevation_id}"
            / "tiles"
            / str(z)
            / str(x)
            / f"{y}.webp"
        )
        try:
            data = await asyncio.to_thread(tile_path.read_bytes)
        except (FileNotFoundError, OSError):
            return None

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
        """List radars from cache or filesystem."""
        cache_key = "cache:listing:radar:radars"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        radars = await asyncio.to_thread(self._scan_subdirs, self._output_path)
        await self._redis.cache_listing(
            cache_key, json.dumps(radars).encode(), self._listing_ttl
        )
        return radars

    async def list_variables(self, radar_id: str) -> List[str]:
        """List variables from cache or filesystem."""
        cache_key = f"cache:listing:radar:{radar_id}:variables"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        path = self._output_path / radar_id
        variables = await asyncio.to_thread(self._scan_subdirs, path)
        await self._redis.cache_listing(
            cache_key, json.dumps(variables).encode(), self._listing_ttl
        )
        return variables

    async def list_elevations(self, radar_id: str, variable_id: str) -> List[str]:
        """List elevations from cache or filesystem."""
        cache_key = f"cache:listing:radar:{radar_id}:{variable_id}:elevations"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        path = self._output_path / radar_id / variable_id
        entries = await asyncio.to_thread(self._scan_subdirs, path)
        elevations = set()
        for entry in entries:
            match = re.search(r"_(elev\d+)$", entry)
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
        """List tilesets from cache or filesystem."""
        cache_key = (
            f"cache:listing:radar:{radar_id}:{variable_id}:{elevation_id}:tilesets"
        )
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        path = self._output_path / radar_id / variable_id
        entries = await asyncio.to_thread(self._scan_subdirs, path)
        tilesets = []
        suffix = f"_{elevation_id}"
        for entry in entries:
            if entry.endswith(suffix):
                tileset_id = entry[: -len(suffix)]
                tilesets.append(tileset_id)
        tilesets.sort(reverse=True)

        await self._redis.cache_listing(
            cache_key, json.dumps(tilesets).encode(), self._listing_ttl
        )
        return tilesets

    @staticmethod
    def _scan_subdirs(path: Path) -> List[str]:
        """Scan immediate subdirectories of a path (blocking I/O)."""
        if not path.is_dir():
            return []
        return sorted(entry.name for entry in path.iterdir() if entry.is_dir())
