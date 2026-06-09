"""Sync strategies for satellite tile retrieval."""

import asyncio
import json
from typing import List, Optional, Protocol

from clients.redis_client import RedisClient
from clients.s3_client import S3Client

# pylint: disable=too-many-arguments,too-many-positional-arguments


class SatelliteSyncStrategy(Protocol):
    """Protocol for satellite sync strategies."""

    async def get_tile(
        self, channel_dir: str, tileset_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile data for the given coordinates."""

    async def get_tilesets(self, channel_dir: str) -> List[str]:
        """Get list of tileset IDs for a channel directory."""


class SatelliteFullSyncStrategy:
    """Redis-first reads (background sync pre-warms), with an S3 fallback.

    A tile miss or an empty (evicted/cold) listing index falls back to S3 — so
    Redis eviction only makes a read slower, never unservable. The S3 miss-path
    is delegated to ``SatelliteOnDemandStrategy`` so the fallback logic lives in
    one place. Without an S3 client the behaviour is Redis-only (unchanged).
    """

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: Optional[S3Client] = None,
        tile_ttl: int = 0,
        listing_ttl: int = 0,
    ):
        self._redis = redis_client
        self._fallback = (
            SatelliteOnDemandStrategy(redis_client, s3_client, tile_ttl, listing_ttl)
            if s3_client is not None
            else None
        )

    async def get_tile(
        self, channel_dir: str, tileset_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile from Redis; on miss fall back to S3 (when configured)."""
        data = await self._redis.get_satellite_tile(channel_dir, tileset_id, z, x, y)
        if data:
            return data
        if self._fallback is not None:
            return await self._fallback.get_tile(channel_dir, tileset_id, z, x, y)
        return None

    async def get_tilesets(self, channel_dir: str) -> List[str]:
        """Get tileset IDs from the live Redis index; on empty fall back to S3."""
        tilesets = await self._redis.get_satellite_tilesets(channel_dir)
        if tilesets:
            return tilesets
        if self._fallback is not None:
            return await self._fallback.get_tilesets(channel_dir)
        return []


class SatelliteOnDemandStrategy:
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
        self, channel_dir: str, tileset_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile from Redis, falling back to S3 with cache-aside."""
        # Always try Redis first
        data = await self._redis.get_satellite_tile(channel_dir, tileset_id, z, x, y)
        if data:
            return data

        # Fall back to S3
        if not self._s3:
            return None

        s3_key = S3Client.build_satellite_tile_key(channel_dir, tileset_id, z, x, y)
        data = await self._s3.download_tile(s3_key)
        if data:
            asyncio.create_task(
                self._redis.store_satellite_tile(
                    channel_dir, tileset_id, z, x, y, data, ttl=self._tile_ttl
                )
            )
            return data

        return None

    async def get_tilesets(self, channel_dir: str) -> List[str]:
        """Get tilesets from cache or S3 listing."""
        cache_key = f"cache:listing:sat:{channel_dir}"

        # Check listing cache
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        # Cache miss: list from S3
        prefix = f"tiles/{channel_dir}/"
        subdirs = await self._s3.get_subdirectories(prefix)

        # Extract tileset IDs from subdirectory prefixes (now just timestamps)
        tileset_ids = []
        for subdir in subdirs:
            name = subdir.rstrip("/").split("/")[-1]
            if name:  # Filter out empty names
                tileset_ids.append(name)

        tileset_ids.sort()

        # Cache with TTL
        await self._redis.cache_listing(
            cache_key, json.dumps(tileset_ids).encode(), self._listing_ttl
        )

        return tileset_ids
