"""Sync strategies for ECMWF precipitation tile retrieval."""

import asyncio
import json
from typing import List, Optional, Protocol

from clients.redis_client import RedisClient
from clients.s3_client import S3Client


class EcmwfSyncStrategy(Protocol):
    """Protocol for ECMWF sync strategies."""

    async def get_tile(
        self, forecast_ts: str, period_ts: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile data for the given forecast/period coordinates."""

    async def list_forecasts(self) -> List[str]:
        """List available forecast timestamps, sorted descending."""

    async def list_periods(self, forecast_ts: str) -> List[str]:
        """List period timestamps for a forecast, sorted ascending."""


class EcmwfFullSyncStrategy:
    """Reads from pre-populated Redis (background sync fills it)."""

    def __init__(self, redis_client: RedisClient):
        self._redis = redis_client

    async def get_tile(
        self, forecast_ts: str, period_ts: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile data from Redis (pre-populated by background sync)."""
        return await self._redis.get_ecmwf_tile(forecast_ts, period_ts, z, x, y)

    async def list_forecasts(self) -> List[str]:
        """List forecast timestamps from the Redis index, sorted descending."""
        return await self._redis.get_ecmwf_forecasts()

    async def list_periods(self, forecast_ts: str) -> List[str]:
        """List period timestamps for a forecast from Redis, sorted ascending."""
        return await self._redis.get_ecmwf_periods(forecast_ts)


class EcmwfOnDemandStrategy:
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
        self, forecast_ts: str, period_ts: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile data: Redis first, fall back to S3 and cache the result."""
        data = await self._redis.get_ecmwf_tile(forecast_ts, period_ts, z, x, y)
        if data:
            return data

        if not self._s3:
            return None

        s3_key = S3Client.build_ecmwf_tile_key(forecast_ts, period_ts, z, x, y)
        data = await self._s3.download_tile(s3_key)
        if data:
            asyncio.create_task(
                self._redis.store_ecmwf_tile(
                    forecast_ts, period_ts, z, x, y, data, ttl=self._tile_ttl
                )
            )
        return data

    async def list_forecasts(self) -> List[str]:
        """List forecast timestamps: cached listing first, fall back to S3."""
        cache_key = "cache:listing:ecmwf:forecasts"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        subdirs = await self._s3.get_subdirectories(S3Client.ECMWF_TILES_PREFIX)
        forecasts = sorted(
            (
                s.rstrip("/").split("/")[-1]
                for s in subdirs
                if s.rstrip("/").split("/")[-1]
            ),
            reverse=True,
        )

        await self._redis.cache_listing(
            cache_key, json.dumps(forecasts).encode(), self._listing_ttl
        )
        return forecasts

    async def list_periods(self, forecast_ts: str) -> List[str]:
        """List period timestamps for a forecast: cached listing first, fall back to S3."""
        cache_key = f"cache:listing:ecmwf:{forecast_ts}:periods"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        prefix = f"{S3Client.ECMWF_TILES_PREFIX}/{forecast_ts}"
        subdirs = await self._s3.get_subdirectories(prefix)
        periods = sorted(
            s.rstrip("/").split("/")[-1]
            for s in subdirs
            if s.rstrip("/").split("/")[-1]
        )

        await self._redis.cache_listing(
            cache_key, json.dumps(periods).encode(), self._listing_ttl
        )
        return periods
