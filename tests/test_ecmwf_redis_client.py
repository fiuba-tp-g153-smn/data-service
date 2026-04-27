"""Unit tests for ECMWF operations in RedisClient."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from clients.redis_client import RedisClient

FORECAST_TS = "20260330T1200Z"
PERIOD_TS = "20260330T1500Z"


@pytest.mark.asyncio
async def test_store_ecmwf_tile_with_ttl():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()

    await client.store_ecmwf_tile(FORECAST_TS, PERIOD_TS, 5, 10, 15, b"data", ttl=86400)

    client._redis.set.assert_awaited_once_with(
        f"tile:ecmwf:{FORECAST_TS}/{PERIOD_TS}/5/10/15", b"data", ex=86400
    )


@pytest.mark.asyncio
async def test_store_ecmwf_tile_without_ttl():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()

    await client.store_ecmwf_tile(FORECAST_TS, PERIOD_TS, 5, 10, 15, b"data")

    client._redis.set.assert_awaited_once_with(
        f"tile:ecmwf:{FORECAST_TS}/{PERIOD_TS}/5/10/15", b"data"
    )


@pytest.mark.asyncio
async def test_get_ecmwf_tile_returns_bytes():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.get = AsyncMock(return_value=b"tile-bytes")

    result = await client.get_ecmwf_tile(FORECAST_TS, PERIOD_TS, 5, 10, 15)

    assert result == b"tile-bytes"
    client._redis.get.assert_awaited_once_with(
        f"tile:ecmwf:{FORECAST_TS}/{PERIOD_TS}/5/10/15"
    )


@pytest.mark.asyncio
async def test_get_ecmwf_tile_returns_none_when_missing():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.get = AsyncMock(return_value=None)

    result = await client.get_ecmwf_tile(FORECAST_TS, PERIOD_TS, 5, 0, 0)

    assert result is None


@pytest.mark.asyncio
async def test_store_ecmwf_index_writes_forecast_and_periods():
    client = RedisClient("redis://localhost:6379/0")
    mock_redis = AsyncMock()
    mock_pipeline = MagicMock()
    mock_redis.pipeline = AsyncMock(return_value=mock_pipeline)
    mock_pipeline.execute = AsyncMock(return_value=[])
    client._redis = mock_redis

    await client.store_ecmwf_index(FORECAST_TS, [PERIOD_TS], ttl=86400)

    mock_pipeline.sadd.assert_any_call("idx:ecmwf:forecasts", FORECAST_TS.encode())
    mock_pipeline.sadd.assert_any_call(
        f"idx:ecmwf:{FORECAST_TS}:periods", PERIOD_TS.encode()
    )
    mock_pipeline.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_ecmwf_forecasts_sorted_desc():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.smembers = AsyncMock(
        return_value={b"20260330T0000Z", b"20260330T1200Z", b"20260329T1200Z"}
    )

    result = await client.get_ecmwf_forecasts()

    assert result == ["20260330T1200Z", "20260330T0000Z", "20260329T1200Z"]
    client._redis.smembers.assert_awaited_once_with("idx:ecmwf:forecasts")


@pytest.mark.asyncio
async def test_get_ecmwf_forecasts_empty():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.smembers = AsyncMock(return_value=set())

    result = await client.get_ecmwf_forecasts()

    assert result == []


@pytest.mark.asyncio
async def test_get_ecmwf_periods_sorted_asc():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.smembers = AsyncMock(
        return_value={
            b"20260330T1500Z",
            b"20260330T1200Z",
        }
    )

    result = await client.get_ecmwf_periods(FORECAST_TS)

    assert result == [
        "20260330T1200Z",
        "20260330T1500Z",
    ]
    client._redis.smembers.assert_awaited_once_with(f"idx:ecmwf:{FORECAST_TS}:periods")


@pytest.mark.asyncio
async def test_get_ecmwf_periods_empty():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.smembers = AsyncMock(return_value=set())

    result = await client.get_ecmwf_periods(FORECAST_TS)

    assert result == []
