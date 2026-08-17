import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from redis.exceptions import ResponseError
from clients.redis_client import RedisClient


@pytest.mark.asyncio
async def test_connect_and_close():
    """Verify connect creates Redis connection and close shuts it down."""
    with patch("clients.redis_client.aioredis") as mock_aioredis:
        mock_redis = AsyncMock()
        mock_aioredis.from_url.return_value = mock_redis

        client = RedisClient(
            "redis://localhost:6379/0",
            max_connections=100,
            socket_timeout_seconds=5.0,
            socket_connect_timeout_seconds=2.0,
            health_check_interval_seconds=30,
        )
        await client.connect()

        mock_aioredis.from_url.assert_called_once_with(
            "redis://localhost:6379/0",
            decode_responses=False,
            max_connections=100,
            socket_timeout=5.0,
            socket_connect_timeout=2.0,
            socket_keepalive=True,
            health_check_interval=30,
        )

        # connect() is idempotent: a second call must not build a second pool.
        await client.connect()
        mock_aioredis.from_url.assert_called_once()

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

    # Tile data must be deleted before the index entry (orphan-on-crash safety):
    # a crash between the two should leave a stale index pointer, never orphaned
    # tile bytes with no index to trim them.
    call_names = [c[0] for c in client._redis.mock_calls if c[0] in ("delete", "zrem")]
    assert call_names.index("delete") < call_names.index("zrem")


@pytest.mark.asyncio
async def test_trim_satellite_index():
    """Verify trim removes index members scored below the cutoff (exclusive)."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.zremrangebyscore = AsyncMock(return_value=3)

    removed = await client.trim_satellite_index("band_13", 1000.0)

    assert removed == 3
    client._redis.zremrangebyscore.assert_awaited_once_with(
        "idx:sat:band_13", "-inf", "(1000.0"
    )


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

    await client.add_radar_index("RMA1", "DBZH", "elev0", "ts1", 1234.0, ttl=3600)
    mock_pipeline.execute.assert_awaited_once()
    # Tilesets axis is a scored sorted set; the dimension axes stay plain sets.
    mock_pipeline.zadd.assert_called_once_with(
        "idx:radar:RMA1:DBZH:elev0:tilesets", {b"ts1": 1234.0}
    )
    assert mock_pipeline.sadd.call_count == 3

    # Test get operations
    mock_redis.smembers = AsyncMock(return_value={b"RMA1", b"RMA2"})
    radars = await client.get_radar_radars()
    assert sorted(radars) == ["RMA1", "RMA2"]


@pytest.mark.asyncio
async def test_get_radar_tilesets_returns_newest_first():
    """Sorted-set members are decoded and returned newest (highest) first."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.zrange = AsyncMock(return_value=[b"ts1", b"ts2", b"ts3"])

    tilesets = await client.get_radar_tilesets("RMA1", "DBZH", "elev0")

    assert tilesets == ["ts3", "ts2", "ts1"]
    client._redis.zrange.assert_awaited_once_with(
        "idx:radar:RMA1:DBZH:elev0:tilesets", 0, -1
    )
    client._redis.delete.assert_not_called()


@pytest.mark.asyncio
async def test_get_radar_tilesets_self_heals_legacy_wrongtype_key():
    """A WRONGTYPE (legacy plain-set key) is dropped and read as empty."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.zrange = AsyncMock(
        side_effect=ResponseError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )
    )

    tilesets = await client.get_radar_tilesets("RMA1", "VRAD", "elev0")

    assert tilesets == []
    client._redis.delete.assert_awaited_once_with("idx:radar:RMA1:VRAD:elev0:tilesets")


@pytest.mark.asyncio
async def test_get_radar_tilesets_reraises_other_response_errors():
    """Non-WRONGTYPE Redis errors propagate instead of being swallowed."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.zrange = AsyncMock(side_effect=ResponseError("LOADING"))

    with pytest.raises(ResponseError):
        await client.get_radar_tilesets("RMA1", "VRAD", "elev0")

    client._redis.delete.assert_not_called()


@pytest.mark.asyncio
async def test_trim_radar_index():
    """Verify radar trim removes tilesets scored below the cutoff (exclusive)."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.zremrangebyscore = AsyncMock(return_value=2)

    removed = await client.trim_radar_index("RMA1", "DBZH", "elev0", 1000.0)

    assert removed == 2
    client._redis.zremrangebyscore.assert_awaited_once_with(
        "idx:radar:RMA1:DBZH:elev0:tilesets", "-inf", "(1000.0"
    )


@pytest.mark.asyncio
async def test_prune_ecmwf_tp_forecasts():
    """Prune removes only forecast members absent from the active keep-list."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.smembers = AsyncMock(return_value={b"f_new", b"f_old1", b"f_old2"})

    removed = await client.prune_ecmwf_tp_forecasts(["f_new"])

    assert removed == 2
    client._redis.srem.assert_awaited_once()
    key, *stale = client._redis.srem.await_args.args
    assert key == "idx:ecmwf_tp:forecasts"
    assert set(stale) == {b"f_old1", b"f_old2"}


@pytest.mark.asyncio
async def test_prune_ecmwf_forecasts_noop_when_all_active():
    """Prune does not call srem when every member is still active."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.smembers = AsyncMock(return_value={b"f_a", b"f_b"})

    removed = await client.prune_ecmwf_mslp_forecasts(["f_a", "f_b"])

    assert removed == 0
    client._redis.srem.assert_not_called()


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


@pytest.mark.asyncio
async def test_prune_wrf_inits_removes_stale_members():
    """prune_wrf_inits ZREMs init_tags not in the active set; keeps the rest."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.zrange = AsyncMock(
        return_value=[b"20260430_060000", b"20260430_000000", b"20260429_180000"]
    )

    removed = await client.prune_wrf_inits("precip", ["20260430_060000"])

    assert removed == 2
    client._redis.zrem.assert_awaited_once_with(
        "idx:wrf:precip:init_runs", b"20260430_000000", b"20260429_180000"
    )


@pytest.mark.asyncio
async def test_prune_wrf_inits_noop_when_all_active():
    """No ZREM when every member is in the active set."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.zrange = AsyncMock(return_value=[b"20260430_060000"])

    removed = await client.prune_wrf_inits("precip", ["20260430_060000"])

    assert removed == 0
    client._redis.zrem.assert_not_awaited()


@pytest.mark.asyncio
async def test_prune_gfs_cycles_uses_the_sorted_set_commands():
    """The cycle index is a ZSET: SMEMBERS/SREM on it answer WRONGTYPE, which
    the sync loop swallows, leaving the index to grow forever."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.zrange = AsyncMock(
        return_value=[b"20260816T0000Z", b"20260815T1800Z", b"20260815T1200Z"]
    )

    removed = await client.prune_gfs_cycles(
        "mslp", ["20260816T0000Z", "20260815T1800Z"]
    )

    assert removed == 1
    client._redis.zrem.assert_awaited_once_with(
        "idx:gfs:mslp:cycles", b"20260815T1200Z"
    )
    client._redis.smembers.assert_not_awaited()


@pytest.mark.asyncio
async def test_prune_gfs_cycles_noop_when_all_active():
    """No ZREM when every indexed cycle is still active."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.zrange = AsyncMock(return_value=[b"20260816T0000Z"])

    removed = await client.prune_gfs_cycles("mslp", ["20260816T0000Z"])

    assert removed == 0
    client._redis.zrem.assert_not_awaited()


@pytest.mark.asyncio
async def test_wrf_overlays_complete_marker_roundtrip():
    """set_wrf_overlays_complete writes a TTL'd key; is_wrf_overlays_complete reads it."""
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    client._redis.exists = AsyncMock(return_value=1)

    await client.set_wrf_overlays_complete("precip", "20260430_060000", "F012", ttl=99)
    client._redis.set.assert_awaited_once_with(
        "idx:wrf:precip:20260430_060000:F012:overlays_done", b"1", ex=99
    )

    assert (
        await client.is_wrf_overlays_complete("precip", "20260430_060000", "F012")
        is True
    )
