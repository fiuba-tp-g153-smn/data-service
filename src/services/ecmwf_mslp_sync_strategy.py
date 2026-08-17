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
    """Redis-first reads (background sync pre-warms), with an S3 fallback.

    GeoJSON and listings fall back to S3 when Redis misses / its index is empty
    (evicted or cold), delegating the S3 miss-path to ``EcmwfMslpOnDemandStrategy``.
    Without an S3 client the behaviour is Redis-only (unchanged).
    """

    def __init__(
        self,
        redis_client: RedisClient,
        s3_client: Optional[S3Client] = None,
        geojson_ttl: int = 0,
        listing_ttl: int = 0,
    ):
        self._redis = redis_client
        self._fallback = (
            EcmwfMslpOnDemandStrategy(redis_client, s3_client, geojson_ttl, listing_ttl)
            if s3_client is not None
            else None
        )

    async def get_geojson(self, forecast_ts: str, timestamp_ts: str) -> Optional[bytes]:
        """Get isobars GeoJSON from Redis; on miss fall back to S3."""
        data = await self._redis.get_ecmwf_mslp_geojson(forecast_ts, timestamp_ts)
        if data:
            return data
        if self._fallback is not None:
            return await self._fallback.get_geojson(forecast_ts, timestamp_ts)
        return None

    async def list_forecasts(self) -> List[str]:
        """List forecasts from the Redis index; on empty fall back to S3."""
        forecasts = await self._redis.get_ecmwf_mslp_forecasts()
        if forecasts:
            return forecasts
        if self._fallback is not None:
            return await self._fallback.list_forecasts()
        return []

    async def list_timestamps(self, forecast_ts: str) -> List[str]:
        """List timestamps from the Redis index; on empty fall back to S3."""
        timestamps = await self._redis.get_ecmwf_mslp_timestamps(forecast_ts)
        if timestamps:
            return timestamps
        if self._fallback is not None:
            return await self._fallback.list_timestamps(forecast_ts)
        return []


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

        subdirs = await self._s3.try_get_subdirectories(S3Client.ECMWF_MSLP_COG_PREFIX)
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
        basenames = await self._s3.try_list_object_basenames(prefix, ".tif")
        timestamps = sorted(b for b in basenames if is_valid_timestamp_format(b))

        await self._redis.cache_listing(
            cache_key, json.dumps(timestamps).encode(), self._listing_ttl
        )
        return timestamps
