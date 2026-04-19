"""Sync strategy for base map tile retrieval with 3-tier fallback."""

import asyncio
import logging
from typing import Optional, Protocol

from clients.http_tile_client import HttpTileClient
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.basemap_config import build_source_url, get_provider

logger = logging.getLogger(__name__)


class BasemapSyncStrategy(Protocol):
    """Protocol for base map sync strategies."""

    async def get_tile(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile data for the given coordinates."""


class BasemapOnDemandStrategy:
    """
    3-tier fallback: Redis → S3 → External provider.

    On S3 hit, caches to Redis asynchronously.
    On external hit, stores to both S3 and Redis.
    """

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: S3Client,
        http_client: HttpTileClient,
        tile_ttl: int,
    ):
        self._redis = redis_client
        self._s3 = s3_client
        self._http = http_client
        self._tile_ttl = tile_ttl

    async def get_tile(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile with Redis → S3 → external fallback."""
        # Tier 1: Redis cache
        data = await self._redis.get_basemap_tile(provider_id, z, x, y)
        if data:
            return data

        s3_key = S3Client.build_basemap_tile_key(provider_id, z, x, y)

        # Tier 2: S3 cold storage
        data = await self._s3.download_tile(s3_key)
        if data:
            asyncio.create_task(
                self._redis.store_basemap_tile(
                    provider_id, z, x, y, data, ttl=self._tile_ttl
                )
            )
            return data

        # Tier 3: External provider (transparent proxy)
        provider = get_provider(provider_id)
        if not provider:
            return None

        url = build_source_url(provider, z, x, y)
        data = await self._http.download_tile(url)
        if data:
            asyncio.create_task(self._cache_tile(provider_id, z, x, y, s3_key, data))
            return data

        return None

    async def _cache_tile(
        self, provider_id: str, z: int, x: int, y: int, s3_key: str, data: bytes
    ) -> None:
        """Store tile in both S3 and Redis (background task)."""
        try:
            await self._s3.upload_tile(s3_key, data)
            await self._redis.store_basemap_tile(
                provider_id, z, x, y, data, ttl=self._tile_ttl
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Failed to cache basemap tile %s: %s", s3_key, exc)
