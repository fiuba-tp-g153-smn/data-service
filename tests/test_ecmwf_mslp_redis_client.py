"""Unit tests for ECMWF MSLP operations in RedisClient."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from clients.redis_client import RedisClient

FORECAST_TS = "20260413T1200Z"
TIMESTAMP_TS = "20260413T1500Z"


@pytest.mark.asyncio
async def test_store_ecmwf_mslp_geojson_with_ttl():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()

    await client.store_ecmwf_mslp_geojson(FORECAST_TS, TIMESTAMP_TS, b"data", ttl=86400)

    client._redis.set.assert_awaited_once_with(
        f"geojson:ecmwf_mslp:{FORECAST_TS}/{TIMESTAMP_TS}", b"data", ex=86400
    )


@pytest.mark.asyncio
async def test_store_ecmwf_mslp_geojson_without_ttl():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()

    await client.store_ecmwf_mslp_geojson(FORECAST_TS, TIMESTAMP_TS, b"data")

    client._redis.set.assert_awaited_once_with(
        f"geojson:ecmwf_mslp:{FORECAST_TS}/{TIMESTAMP_TS}", b"data"
    )


@pytest.mark.asyncio
async def test_get_ecmwf_mslp_geojson_returns_bytes():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.get = AsyncMock(return_value=b'{"type":"FeatureCollection"}')

    result = await client.get_ecmwf_mslp_geojson(FORECAST_TS, TIMESTAMP_TS)

    assert result == b'{"type":"FeatureCollection"}'
    client._redis.get.assert_awaited_once_with(
        f"geojson:ecmwf_mslp:{FORECAST_TS}/{TIMESTAMP_TS}"
    )


@pytest.mark.asyncio
async def test_get_ecmwf_mslp_geojson_returns_none_when_missing():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.get = AsyncMock(return_value=None)

    result = await client.get_ecmwf_mslp_geojson(FORECAST_TS, TIMESTAMP_TS)

    assert result is None


@pytest.mark.asyncio
async def test_store_ecmwf_mslp_index_writes_forecast_and_timestamps():
    client = RedisClient("redis://localhost:6379/0")
    mock_pipeline = MagicMock()
    mock_pipeline.execute = AsyncMock()
    client._redis = AsyncMock()
    client._redis.pipeline = AsyncMock(return_value=mock_pipeline)

    await client.store_ecmwf_mslp_index(FORECAST_TS, [TIMESTAMP_TS], ttl=86400)

    mock_pipeline.sadd.assert_any_call("idx:ecmwf_mslp:forecasts", FORECAST_TS.encode())
    mock_pipeline.sadd.assert_any_call(
        f"idx:ecmwf_mslp:{FORECAST_TS}:timestamps", TIMESTAMP_TS.encode()
    )
    mock_pipeline.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_ecmwf_mslp_forecasts_sorted_desc():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.smembers = AsyncMock(
        return_value={b"20260413T0000Z", b"20260413T1200Z", b"20260412T1200Z"}
    )

    result = await client.get_ecmwf_mslp_forecasts()

    assert result == ["20260413T1200Z", "20260413T0000Z", "20260412T1200Z"]
    client._redis.smembers.assert_awaited_once_with("idx:ecmwf_mslp:forecasts")


@pytest.mark.asyncio
async def test_get_ecmwf_mslp_timestamps_sorted_asc():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.smembers = AsyncMock(
        return_value={
            b"20260413T1800Z",
            b"20260413T1500Z",
            b"20260413T2100Z",
        }
    )

    result = await client.get_ecmwf_mslp_timestamps(FORECAST_TS)

    assert result == ["20260413T1500Z", "20260413T1800Z", "20260413T2100Z"]
    client._redis.smembers.assert_awaited_once_with(
        f"idx:ecmwf_mslp:{FORECAST_TS}:timestamps"
    )


@pytest.mark.asyncio
async def test_get_ecmwf_mslp_timestamps_empty():
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.smembers = AsyncMock(return_value=set())

    result = await client.get_ecmwf_mslp_timestamps(FORECAST_TS)

    assert result == []
