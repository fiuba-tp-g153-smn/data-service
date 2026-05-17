"""Sync strategies for ECMWF mean sea level pressure (COG + isobars GeoJSON)."""

import asyncio
import json
from typing import List, Optional, Protocol

from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.ecmwf_tp_sync_strategy import is_valid_timestamp_format


class EcmwfMslpSyncStrategy(Protocol):
    """Protocol for ECMWF mean sea level pressure sync strategies."""

    async def get_geojson(self, forecast_ts: str, timestamp_ts: str) -> Optional[bytes]:
        """Get isobars GeoJSON for the given forecast/timestamp."""

    async def list_forecasts(self) -> List[str]:
        """List available MSLP forecast timestamps, sorted descending."""

    async def list_timestamps(self, forecast_ts: str) -> List[str]:
        """List MSLP timestamps for a forecast, sorted ascending."""


class EcmwfMslpFullSyncStrategy:
    """Reads from pre-populated Redis (background sync fills it)."""

    def __init__(self, redis_client: RedisClient):
        self._redis = redis_client

    async def get_geojson(self, forecast_ts: str, timestamp_ts: str) -> Optional[bytes]:
        """Get isobars GeoJSON from Redis (pre-populated by background sync)."""
        return await self._redis.get_ecmwf_mslp_geojson(forecast_ts, timestamp_ts)

    async def list_forecasts(self) -> List[str]:
        """List forecast timestamps from the Redis index, sorted descending."""
        return await self._redis.get_ecmwf_mslp_forecasts()

    async def list_timestamps(self, forecast_ts: str) -> List[str]:
        """List MSLP timestamps for a forecast from Redis, sorted ascending."""
        return await self._redis.get_ecmwf_mslp_timestamps(forecast_ts)


class EcmwfMslpOnDemandStrategy:
    """Tries Redis first, falls back to S3, caches with TTL."""

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: Optional[S3Client],
        geojson_ttl: int,
        listing_ttl: int,
    ):
        self._redis = redis_client
        self._s3 = s3_client
        self._geojson_ttl = geojson_ttl
        self._listing_ttl = listing_ttl

    async def get_geojson(self, forecast_ts: str, timestamp_ts: str) -> Optional[bytes]:
        """Get isobars GeoJSON: Redis first, fall back to S3 and cache the result."""
        data = await self._redis.get_ecmwf_mslp_geojson(forecast_ts, timestamp_ts)
        if data:
            return data

        if not self._s3:
            return None

        s3_key = S3Client.build_ecmwf_mslp_geojson_key(forecast_ts, timestamp_ts)
        data = await self._s3.download_tile(s3_key)
        if data:
            asyncio.create_task(
                self._redis.store_ecmwf_mslp_geojson(
                    forecast_ts, timestamp_ts, data, ttl=self._geojson_ttl
                )
            )
        return data

    async def list_forecasts(self) -> List[str]:
        """List MSLP forecast timestamps: cached listing first, fall back to S3."""
        cache_key = "cache:listing:ecmwf_mslp:forecasts"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        subdirs = await self._s3.get_subdirectories(S3Client.ECMWF_MSLP_COG_PREFIX)
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

    async def list_timestamps(self, forecast_ts: str) -> List[str]:
        """List MSLP timestamps for a forecast: cached listing first, fall back to S3."""
        cache_key = f"cache:listing:ecmwf_mslp:{forecast_ts}:timestamps"
        cached = await self._redis.get_cached_listing(cache_key)
        if cached:
            return json.loads(cached)

        if not self._s3:
            return []

        prefix = f"{S3Client.ECMWF_MSLP_COG_PREFIX}/{forecast_ts}/"
        basenames = await self._s3.list_object_basenames(prefix, ".tif")
        timestamps = sorted(b for b in basenames if is_valid_timestamp_format(b))

        await self._redis.cache_listing(
            cache_key, json.dumps(timestamps).encode(), self._listing_ttl
        )
        return timestamps
