import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from clients.redis_client import RedisClient


@pytest.mark.asyncio
async def test_connect_and_close():
    """Verify connect creates Redis connection and close shuts it down."""
    with patch("clients.redis_client.aioredis") as mock_aioredis:
        mock_redis = AsyncMock()
        mock_aioredis.from_url.return_value = mock_redis

        client = RedisClient("redis://localhost:6379/0")
        await client.connect()

        mock_aioredis.from_url.assert_called_once_with(
            "redis://localhost:6379/0", decode_responses=False
        )

        await client.close()
        mock_redis.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_health_check_success():
    """Verify health_check returns True when Redis is reachable."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.ping = AsyncMock(return_value=True)

    result = await client.health_check()
    assert result is True


@pytest.mark.asyncio
async def test_health_check_failure():
    """Verify health_check returns False when Redis is unreachable."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.ping = AsyncMock(side_effect=ConnectionError("refused"))

    result = await client.health_check()
    assert result is False


@pytest.mark.asyncio
async def test_store_and_get_satellite_tile():
    """Verify satellite tile store and retrieval."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()

    tile_data = b"fake-webp-data"
    client._redis.get = AsyncMock(return_value=tile_data)

    await client.store_satellite_tile("band_13", "tileset1", 5, 10, 15, tile_data)
    client._redis.set.assert_awaited_once_with(
        "tile:sat:band_13/tileset1/5/10/15", tile_data
    )

    result = await client.get_satellite_tile("band_13", "tileset1", 5, 10, 15)
    assert result == tile_data


@pytest.mark.asyncio
async def test_satellite_tileset_index():
    """Verify add and get for satellite tileset index."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.zrange = AsyncMock(return_value=[b"tileset1", b"tileset2"])

    await client.add_satellite_tileset("band_13", "tileset1", 20250141230210.0)
    client._redis.zadd.assert_awaited_once()

    tilesets = await client.get_satellite_tilesets("band_13")
    assert tilesets == ["tileset1", "tileset2"]


@pytest.mark.asyncio
async def test_delete_satellite_tileset():
    """Verify tileset deletion removes index entry and tile keys."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    # Simulate scan returning some keys then finishing
    client._redis.scan = AsyncMock(
        return_value=(0, [b"tile:sat:band_13/tileset1/5/10/15"])
    )

    await client.delete_satellite_tileset("band_13", "tileset1")

    client._redis.zrem.assert_awaited_once()
    client._redis.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_store_radar_tile_with_ttl():
    """Verify radar tile is stored with TTL."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()

    await client.store_radar_tile(
        "RMA1", "DBZH", "ts1", "elev0", 5, 10, 15, b"data", ttl=3600
    )
    client._redis.set.assert_awaited_once_with(
        "tile:radar:RMA1/DBZH/ts1_elev0/5/10/15", b"data", ex=3600
    )


@pytest.mark.asyncio
async def test_radar_index_operations():
    """Verify radar index add and query operations."""
    client = RedisClient("redis://localhost:6379/0")
    mock_redis = AsyncMock()
    mock_pipeline = MagicMock()
    mock_redis.pipeline = AsyncMock(return_value=mock_pipeline)
    mock_pipeline.execute = AsyncMock(return_value=[])
    client._redis = mock_redis

    await client.add_radar_index("RMA1", "DBZH", "elev0", "ts1", ttl=3600)
    mock_pipeline.execute.assert_awaited_once()

    # Test get operations
    mock_redis.smembers = AsyncMock(return_value={b"RMA1", b"RMA2"})
    radars = await client.get_radar_radars()
    assert sorted(radars) == ["RMA1", "RMA2"]


@pytest.mark.asyncio
async def test_basemap_provider_availability_round_trip():
    """Availability writes a 1/0 byte under the expected key with TTL."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.get = AsyncMock(side_effect=[b"1", b"0", None])

    await client.set_basemap_provider_availability("argenmap", True, ttl=240)
    client._redis.set.assert_awaited_once_with(
        "basemap:availability:argenmap", b"1", ex=240
    )

    available = await client.get_basemap_provider_availability("argenmap")
    assert available is True

    unavailable = await client.get_basemap_provider_availability("argenmap")
    assert unavailable is False

    missing = await client.get_basemap_provider_availability("argenmap")
    assert missing is None


@pytest.mark.asyncio
async def test_sync_status_operations():
    """Verify sync status update and retrieval."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.hgetall = AsyncMock(
        return_value={
            b"is_running": b"false",
            b"total_cycles": b"5",
            b"last_sync_duration_ms": b"1234",
        }
    )

    await client.update_sync_status({"is_running": "true", "total_cycles": "6"})
    client._redis.hset.assert_awaited_once()

    status = await client.get_sync_status()
    assert status["is_running"] == "false"
    assert status["total_cycles"] == "5"
