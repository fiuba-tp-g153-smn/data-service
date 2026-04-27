"""Unit tests for ECMWF MSLP sync strategies (full and on-demand)."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from clients.s3_client import S3Client
from services.ecmwf_mslp_sync_strategy import (
    EcmwfMslpFullSyncStrategy,
    EcmwfMslpOnDemandStrategy,
)

FORECAST_TS = "20260413T1200Z"
TIMESTAMP_TS = "20260413T1500Z"
GEOJSON_BYTES = b'{"type":"FeatureCollection","features":[]}'


# ── EcmwfMslpFullSyncStrategy ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_full_strategy_get_geojson_delegates_to_redis(mock_redis_client):
    mock_redis_client.get_ecmwf_mslp_geojson = AsyncMock(return_value=GEOJSON_BYTES)
    strategy = EcmwfMslpFullSyncStrategy(mock_redis_client)

    result = await strategy.get_geojson(FORECAST_TS, TIMESTAMP_TS)

    assert result == GEOJSON_BYTES
    mock_redis_client.get_ecmwf_mslp_geojson.assert_awaited_once_with(
        FORECAST_TS, TIMESTAMP_TS
    )


@pytest.mark.asyncio
async def test_full_strategy_list_forecasts(mock_redis_client):
    mock_redis_client.get_ecmwf_mslp_forecasts = AsyncMock(
        return_value=[FORECAST_TS, "20260413T0000Z"]
    )
    strategy = EcmwfMslpFullSyncStrategy(mock_redis_client)

    result = await strategy.list_forecasts()

    assert result == [FORECAST_TS, "20260413T0000Z"]


@pytest.mark.asyncio
async def test_full_strategy_list_timestamps(mock_redis_client):
    mock_redis_client.get_ecmwf_mslp_timestamps = AsyncMock(return_value=[TIMESTAMP_TS])
    strategy = EcmwfMslpFullSyncStrategy(mock_redis_client)

    result = await strategy.list_timestamps(FORECAST_TS)

    assert result == [TIMESTAMP_TS]


# ── EcmwfMslpOnDemandStrategy ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_on_demand_get_geojson_redis_hit(mock_redis_client):
    mock_redis_client.get_ecmwf_mslp_geojson = AsyncMock(return_value=GEOJSON_BYTES)
    strategy = EcmwfMslpOnDemandStrategy(mock_redis_client, None, 3600, 30)

    result = await strategy.get_geojson(FORECAST_TS, TIMESTAMP_TS)

    assert result == GEOJSON_BYTES


@pytest.mark.asyncio
async def test_on_demand_get_geojson_s3_fallback(mock_redis_client):
    mock_redis_client.get_ecmwf_mslp_geojson = AsyncMock(return_value=None)

    mock_s3 = MagicMock()
    mock_s3.download_tile = AsyncMock(return_value=GEOJSON_BYTES)

    strategy = EcmwfMslpOnDemandStrategy(mock_redis_client, mock_s3, 3600, 30)
    result = await strategy.get_geojson(FORECAST_TS, TIMESTAMP_TS)

    # Allow the asyncio.create_task background write to schedule.
    await asyncio.sleep(0)

    assert result == GEOJSON_BYTES
    expected_key = S3Client.build_ecmwf_mslp_geojson_key(FORECAST_TS, TIMESTAMP_TS)
    mock_s3.download_tile.assert_awaited_once_with(expected_key)


@pytest.mark.asyncio
async def test_on_demand_get_geojson_no_s3_returns_none(mock_redis_client):
    mock_redis_client.get_ecmwf_mslp_geojson = AsyncMock(return_value=None)
    strategy = EcmwfMslpOnDemandStrategy(mock_redis_client, None, 3600, 30)

    result = await strategy.get_geojson(FORECAST_TS, TIMESTAMP_TS)
    assert result is None


@pytest.mark.asyncio
async def test_on_demand_list_forecasts_cache_hit(mock_redis_client):
    forecasts = [FORECAST_TS, "20260413T0000Z"]
    mock_redis_client.get_cached_listing = AsyncMock(
        return_value=json.dumps(forecasts).encode()
    )
    strategy = EcmwfMslpOnDemandStrategy(mock_redis_client, None, 3600, 30)

    result = await strategy.list_forecasts()

    assert result == forecasts
    mock_redis_client.cache_listing.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_demand_list_forecasts_s3_fallback(mock_redis_client):
    mock_redis_client.get_cached_listing = AsyncMock(return_value=None)

    mock_s3 = MagicMock()
    mock_s3.get_subdirectories = AsyncMock(
        return_value=[
            f"{S3Client.ECMWF_MSLP_COG_PREFIX}/20260413T1200Z/",
            f"{S3Client.ECMWF_MSLP_COG_PREFIX}/20260413T0000Z/",
        ]
    )

    strategy = EcmwfMslpOnDemandStrategy(mock_redis_client, mock_s3, 3600, 30)

    result = await strategy.list_forecasts()

    assert result == ["20260413T1200Z", "20260413T0000Z"]
    mock_redis_client.cache_listing.assert_awaited_once()


@pytest.mark.asyncio
async def test_on_demand_list_timestamps_s3_fallback(mock_redis_client):
    mock_redis_client.get_cached_listing = AsyncMock(return_value=None)

    mock_s3 = MagicMock()
    mock_s3.list_object_basenames = AsyncMock(
        return_value=["20260413T1500Z", "20260413T1800Z", "metadata"]
    )

    strategy = EcmwfMslpOnDemandStrategy(mock_redis_client, mock_s3, 3600, 30)

    result = await strategy.list_timestamps(FORECAST_TS)

    # "metadata" is filtered by is_centered_period_format
    assert result == ["20260413T1500Z", "20260413T1800Z"]
    mock_redis_client.cache_listing.assert_awaited_once()


@pytest.mark.asyncio
async def test_on_demand_list_timestamps_no_s3_returns_empty(mock_redis_client):
    mock_redis_client.get_cached_listing = AsyncMock(return_value=None)
    strategy = EcmwfMslpOnDemandStrategy(mock_redis_client, None, 3600, 30)

    result = await strategy.list_timestamps(FORECAST_TS)

    assert result == []
