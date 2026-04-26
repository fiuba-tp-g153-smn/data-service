"""Unit tests for ECMWF sync strategies (full and on-demand)."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from clients.s3_client import S3Client
from services.ecmwf_sync_strategy import (
    EcmwfFullSyncStrategy,
    EcmwfOnDemandStrategy,
    is_centered_period_format,
)

FORECAST_TS = "20260330T1200Z"
PERIOD_TS = "20260330T1500Z"
TILE_DATA = b"webp-bytes"


# ── EcmwfFullSyncStrategy ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_full_strategy_get_tile_delegates_to_redis(mock_redis_client):
    mock_redis_client.get_ecmwf_tile = AsyncMock(return_value=TILE_DATA)
    strategy = EcmwfFullSyncStrategy(mock_redis_client)

    result = await strategy.get_tile(FORECAST_TS, PERIOD_TS, 5, 10, 15)

    assert result == TILE_DATA
    mock_redis_client.get_ecmwf_tile.assert_awaited_once_with(
        FORECAST_TS, PERIOD_TS, 5, 10, 15
    )


@pytest.mark.asyncio
async def test_full_strategy_get_tile_returns_none_on_miss(mock_redis_client):
    mock_redis_client.get_ecmwf_tile = AsyncMock(return_value=None)
    strategy = EcmwfFullSyncStrategy(mock_redis_client)

    result = await strategy.get_tile(FORECAST_TS, PERIOD_TS, 5, 0, 0)

    assert result is None


@pytest.mark.asyncio
async def test_full_strategy_list_forecasts(mock_redis_client):
    mock_redis_client.get_ecmwf_forecasts = AsyncMock(
        return_value=[FORECAST_TS, "20260330T0000Z"]
    )
    strategy = EcmwfFullSyncStrategy(mock_redis_client)

    result = await strategy.list_forecasts()

    assert result == [FORECAST_TS, "20260330T0000Z"]


@pytest.mark.asyncio
async def test_full_strategy_list_periods(mock_redis_client):
    mock_redis_client.get_ecmwf_periods = AsyncMock(return_value=[PERIOD_TS])
    strategy = EcmwfFullSyncStrategy(mock_redis_client)

    result = await strategy.list_periods(FORECAST_TS)

    assert result == [PERIOD_TS]
    mock_redis_client.get_ecmwf_periods.assert_awaited_once_with(FORECAST_TS)


# ── EcmwfOnDemandStrategy ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_on_demand_get_tile_redis_hit(mock_redis_client):
    mock_redis_client.get_ecmwf_tile = AsyncMock(return_value=TILE_DATA)
    strategy = EcmwfOnDemandStrategy(mock_redis_client, None, 3600, 30)

    result = await strategy.get_tile(FORECAST_TS, PERIOD_TS, 5, 10, 15)

    assert result == TILE_DATA


@pytest.mark.asyncio
async def test_on_demand_get_tile_s3_fallback(mock_redis_client):
    mock_redis_client.get_ecmwf_tile = AsyncMock(return_value=None)

    mock_s3 = MagicMock()
    mock_s3.download_tile = AsyncMock(return_value=TILE_DATA)

    strategy = EcmwfOnDemandStrategy(mock_redis_client, mock_s3, 3600, 30)

    result = await strategy.get_tile(FORECAST_TS, PERIOD_TS, 5, 10, 15)

    assert result == TILE_DATA
    expected_key = S3Client.build_ecmwf_tile_key(FORECAST_TS, PERIOD_TS, 5, 10, 15)
    mock_s3.download_tile.assert_awaited_once_with(expected_key)


@pytest.mark.asyncio
async def test_on_demand_get_tile_no_s3_returns_none(mock_redis_client):
    mock_redis_client.get_ecmwf_tile = AsyncMock(return_value=None)
    strategy = EcmwfOnDemandStrategy(mock_redis_client, None, 3600, 30)

    result = await strategy.get_tile(FORECAST_TS, PERIOD_TS, 5, 0, 0)

    assert result is None


@pytest.mark.asyncio
async def test_on_demand_get_tile_s3_miss_returns_none(mock_redis_client):
    mock_redis_client.get_ecmwf_tile = AsyncMock(return_value=None)

    mock_s3 = MagicMock()
    mock_s3.download_tile = AsyncMock(return_value=None)

    strategy = EcmwfOnDemandStrategy(mock_redis_client, mock_s3, 3600, 30)

    result = await strategy.get_tile(FORECAST_TS, PERIOD_TS, 5, 0, 0)

    assert result is None


@pytest.mark.asyncio
async def test_on_demand_list_forecasts_cache_hit(mock_redis_client):
    forecasts = [FORECAST_TS, "20260330T0000Z"]
    mock_redis_client.get_cached_listing = AsyncMock(
        return_value=json.dumps(forecasts).encode()
    )
    strategy = EcmwfOnDemandStrategy(mock_redis_client, None, 3600, 30)

    result = await strategy.list_forecasts()

    assert result == forecasts
    mock_redis_client.cache_listing.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_demand_list_forecasts_s3_fallback(mock_redis_client):
    mock_redis_client.get_cached_listing = AsyncMock(return_value=None)

    mock_s3 = MagicMock()
    mock_s3.get_subdirectories = AsyncMock(
        return_value=[
            f"{S3Client.ECMWF_TILES_PREFIX}/20260330T1200Z/",
            f"{S3Client.ECMWF_TILES_PREFIX}/20260330T0000Z/",
        ]
    )

    strategy = EcmwfOnDemandStrategy(mock_redis_client, mock_s3, 3600, 30)

    result = await strategy.list_forecasts()

    assert result == ["20260330T1200Z", "20260330T0000Z"]  # sorted desc
    mock_redis_client.cache_listing.assert_awaited_once()


@pytest.mark.asyncio
async def test_on_demand_list_forecasts_no_s3_returns_empty(mock_redis_client):
    mock_redis_client.get_cached_listing = AsyncMock(return_value=None)
    strategy = EcmwfOnDemandStrategy(mock_redis_client, None, 3600, 30)

    result = await strategy.list_forecasts()

    assert result == []


@pytest.mark.asyncio
async def test_on_demand_list_periods_cache_hit(mock_redis_client):
    periods = [PERIOD_TS]
    mock_redis_client.get_cached_listing = AsyncMock(
        return_value=json.dumps(periods).encode()
    )
    strategy = EcmwfOnDemandStrategy(mock_redis_client, None, 3600, 30)

    result = await strategy.list_periods(FORECAST_TS)

    assert result == periods
    mock_redis_client.cache_listing.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_demand_list_periods_s3_fallback(mock_redis_client):
    mock_redis_client.get_cached_listing = AsyncMock(return_value=None)

    mock_s3 = MagicMock()
    mock_s3.get_subdirectories = AsyncMock(
        return_value=[
            f"{S3Client.ECMWF_TILES_PREFIX}/{FORECAST_TS}/{PERIOD_TS}/",
        ]
    )

    strategy = EcmwfOnDemandStrategy(mock_redis_client, mock_s3, 3600, 30)

    result = await strategy.list_periods(FORECAST_TS)

    assert result == [PERIOD_TS]
    mock_redis_client.cache_listing.assert_awaited_once()


@pytest.mark.asyncio
async def test_on_demand_list_periods_no_s3_returns_empty(mock_redis_client):
    mock_redis_client.get_cached_listing = AsyncMock(return_value=None)
    strategy = EcmwfOnDemandStrategy(mock_redis_client, None, 3600, 30)

    result = await strategy.list_periods(FORECAST_TS)

    assert result == []


@pytest.mark.asyncio
async def test_on_demand_list_periods_filters_old_format(mock_redis_client):
    """On-demand listing drops legacy {start}-{end} period IDs from S3."""
    mock_redis_client.get_cached_listing = AsyncMock(return_value=None)

    mock_s3 = MagicMock()
    mock_s3.get_subdirectories = AsyncMock(
        return_value=[
            f"{S3Client.ECMWF_TILES_PREFIX}/{FORECAST_TS}/20260330T1500Z/",
            f"{S3Client.ECMWF_TILES_PREFIX}/{FORECAST_TS}/20260330T1200Z-20260330T1500Z/",
            f"{S3Client.ECMWF_TILES_PREFIX}/{FORECAST_TS}/20260330T1800Z/",
        ]
    )

    strategy = EcmwfOnDemandStrategy(mock_redis_client, mock_s3, 3600, 30)

    result = await strategy.list_periods(FORECAST_TS)

    assert result == ["20260330T1500Z", "20260330T1800Z"]


# ── is_centered_period_format ─────────────────────────────────────────────────


def test_is_centered_period_format_accepts_new_format():
    assert is_centered_period_format("20260330T1500Z") is True
    assert is_centered_period_format("20260101T0000Z") is True


def test_is_centered_period_format_rejects_legacy_and_garbage():
    assert is_centered_period_format("20260330T1500Z-20260330T1800Z") is False
    assert is_centered_period_format("") is False
    assert is_centered_period_format("garbage") is False
    assert is_centered_period_format("20260330") is False
    assert is_centered_period_format("20260330T1500") is False
    assert is_centered_period_format("2026033T1500Z") is False  # short date
