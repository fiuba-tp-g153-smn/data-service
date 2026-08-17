"""Sync strategies for ECMWF total precipitation tile retrieval."""

import asyncio
import json
import re
from typing import List, Optional, Protocol

from clients.redis_client import RedisClient
from clients.s3_client import S3Client

_TIMESTAMP_PATTERN = re.compile(r"^\d{8}T\d{4}Z$")


def is_valid_timestamp_format(period_ts: str) -> bool:
    """Return True if period_ts matches the YYYYMMDDTHHmmZ single-timestamp format."""
    return bool(_TIMESTAMP_PATTERN.fullmatch(period_ts))


class EcmwfTpSyncStrategy(Protocol):
    """Protocol for ECMWF total precipitation sync strategies."""

    async def get_tile(
        self, forecast_ts: str, period_ts: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile data for the given forecast/period coordinates."""

    async def list_forecasts(self) -> List[str]:
        """List available forecast timestamps, sorted descending."""

    async def list_periods(self, forecast_ts: str) -> List[str]:
        """List period timestamps for a forecast, sorted ascending."""


class EcmwfTpFullSyncStrategy:
    """Redis-first reads (background sync pre-warms), with an S3 fallback.

    Tiles and listings fall back to S3 when Redis misses / its index is empty
    (evicted or cold), delegating the S3 miss-path to ``EcmwfTpOnDemandStrategy``.
    Without an S3 client the behaviour is Redis-only (unchanged).
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
            EcmwfTpOnDemandStrategy(redis_client, s3_client, tile_ttl, listing_ttl)
            if s3_client is not None
            else None
        )

    async def get_tile(
        self, forecast_ts: str, period_ts: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile from Redis; on miss fall back to S3."""
        data = await self._redis.get_ecmwf_tp_tile(forecast_ts, period_ts, z, x, y)
        if data:
            return data
        if self._fallback is not None:
            return await self._fallback.get_tile(forecast_ts, period_ts, z, x, y)
        return None

    async def list_forecasts(self) -> List[str]:
        """List forecasts from the Redis index; on empty fall back to S3."""
        forecasts = await self._redis.get_ecmwf_tp_forecasts()
        if forecasts:
            return forecasts
        if self._fallback is not None:
            return await self._fallback.list_forecasts()
        return []

    async def list_periods(self, forecast_ts: str) -> List[str]:
        """List periods from the Redis index; on empty fall back to S3."""
        periods = await self._redis.get_ecmwf_tp_periods(forecast_ts)
        if periods:
            return periods
        if self._fallback is not None:
            return await self._fallback.list_periods(forecast_ts)
        return []


class EcmwfTpOnDemandStrategy:
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
        data = await self._redis.get_ecmwf_tp_tile(forecast_ts, period_ts, z, x, y)
        if data:
            return data

        if not self._s3:
            return None

        s3_key = S3Client.build_ecmwf_tp_tile_key(forecast_ts, period_ts, z, x, y)
        data = await self._s3.download_tile(s3_key)
        if data:
            asyncio.create_task(
                self._redis.store_ecmwf_tp_tile(
                    forecast_ts, period_ts, z, x, y, data, ttl=self._tile_ttl
                )
            )
        return data

    async def list_forecasts(self) -> List[str]:
        """List forecast timestamps: cached listing first, fall back to S3."""
        cache_key = "cache:listing:ecmwf_tp:forecasts"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        subdirs = await self._s3.try_get_subdirectories(S3Client.ECMWF_TP_TILES_PREFIX)
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
        cache_key = f"cache:listing:ecmwf_tp:{forecast_ts}:periods"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        prefix = f"{S3Client.ECMWF_TP_TILES_PREFIX}/{forecast_ts}"
        subdirs = await self._s3.try_get_subdirectories(prefix)
        periods = sorted(
            s.rstrip("/").split("/")[-1]
            for s in subdirs
            if s.rstrip("/").split("/")[-1]
            and is_valid_timestamp_format(s.rstrip("/").split("/")[-1])
        )

        await self._redis.cache_listing(
            cache_key, json.dumps(periods).encode(), self._listing_ttl
        )
        return periods
