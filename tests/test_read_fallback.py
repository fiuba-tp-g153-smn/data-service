"""Read-path S3 fallback tests for the FULL (Redis-first) sync strategies.

Each FULL strategy is given an S3 client so a tile/geojson miss or an empty
(evicted/cold) listing index falls back to S3 instead of returning empty. The
conftest ``mock_redis_client`` defaults to miss/empty for every read, which is
exactly the precondition that triggers the fallback. With ``s3_client=None`` the
strategy stays Redis-only (covered by the existing per-strategy tests).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from clients.s3_client import S3Client
from services.ecmwf_mslp_sync_strategy import EcmwfMslpFullSyncStrategy
from services.ecmwf_tp_sync_strategy import EcmwfTpFullSyncStrategy
from services.radar_sync_strategy import RadarFullSyncStrategy
from services.satellite_sync_strategy import SatelliteFullSyncStrategy
from services.wrf_sync_strategy import WrfFullSyncStrategy

TILE = b"webp-bytes"
GEOJSON = b'{"type":"FeatureCollection"}'


def _s3_with_tile(data=TILE) -> MagicMock:
    s3 = MagicMock()
    s3.download_tile = AsyncMock(return_value=data)
    return s3


def _s3_listing(prefixes) -> MagicMock:
    s3 = MagicMock()
    # Read strategies use the tolerant listing variant (degrades to [] on an S3
    # blip instead of 5xx); the raising get_subdirectories is for the sync loops.
    s3.try_get_subdirectories = AsyncMock(return_value=prefixes)
    return s3


# ── Satellite ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_satellite_tile_falls_back_to_s3(mock_redis_client):
    s3 = _s3_with_tile()
    strategy = SatelliteFullSyncStrategy(mock_redis_client, s3, 3600, 30)

    result = await strategy.get_tile("band_13", "20260101T0000Z", 5, 10, 15)

    assert result == TILE
    s3.download_tile.assert_awaited_once_with(
        S3Client.build_satellite_tile_key("band_13", "20260101T0000Z", 5, 10, 15)
    )


@pytest.mark.asyncio
async def test_satellite_tile_no_s3_returns_none(mock_redis_client):
    strategy = SatelliteFullSyncStrategy(mock_redis_client)  # Redis-only
    assert await strategy.get_tile("band_13", "ts", 5, 0, 0) is None


@pytest.mark.asyncio
async def test_satellite_listing_falls_back_to_s3(mock_redis_client):
    s3 = _s3_listing(["tiles/band_13/20260101T0000Z/", "tiles/band_13/20260101T0010Z/"])
    strategy = SatelliteFullSyncStrategy(mock_redis_client, s3, 3600, 30)

    result = await strategy.get_tilesets("band_13")

    assert result == ["20260101T0000Z", "20260101T0010Z"]


# ── Radar ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_radar_tile_falls_back_to_s3(mock_redis_client):
    s3 = _s3_with_tile()
    strategy = RadarFullSyncStrategy(mock_redis_client, s3, 2592000, 30)

    result = await strategy.get_tile("RMA1", "ZDR", "elev0", "ts", 5, 10, 15)

    assert result == TILE
    s3.download_tile.assert_awaited_once_with(
        S3Client.build_radar_tile_key("RMA1", "ZDR", "ts", "elev0", 5, 10, 15)
    )


@pytest.mark.asyncio
async def test_radar_list_radars_falls_back_to_s3(mock_redis_client):
    s3 = _s3_listing(["tiles/radar/RMA1/", "tiles/radar/RMA2/"])
    strategy = RadarFullSyncStrategy(mock_redis_client, s3, 2592000, 30)

    assert await strategy.list_radars() == ["RMA1", "RMA2"]


# ── ECMWF total precipitation ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ecmwf_tp_tile_falls_back_to_s3(mock_redis_client):
    s3 = _s3_with_tile()
    strategy = EcmwfTpFullSyncStrategy(mock_redis_client, s3, 86400, 30)

    result = await strategy.get_tile("20260330T1200Z", "20260330T1500Z", 5, 10, 15)

    assert result == TILE
    s3.download_tile.assert_awaited_once_with(
        S3Client.build_ecmwf_tp_tile_key("20260330T1200Z", "20260330T1500Z", 5, 10, 15)
    )


@pytest.mark.asyncio
async def test_ecmwf_tp_list_forecasts_falls_back_to_s3(mock_redis_client):
    s3 = _s3_listing(
        [
            f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T1200Z/",
            f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T0000Z/",
        ]
    )
    strategy = EcmwfTpFullSyncStrategy(mock_redis_client, s3, 86400, 30)

    # Sorted descending by the on-demand strategy.
    assert await strategy.list_forecasts() == ["20260330T1200Z", "20260330T0000Z"]


# ── ECMWF mean sea level pressure ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ecmwf_mslp_geojson_falls_back_to_s3(mock_redis_client):
    s3 = _s3_with_tile(GEOJSON)
    strategy = EcmwfMslpFullSyncStrategy(mock_redis_client, s3, 86400, 30)

    result = await strategy.get_geojson("20260330T1200Z", "20260330T1500Z")

    assert result == GEOJSON
    s3.download_tile.assert_awaited_once_with(
        S3Client.build_ecmwf_mslp_geojson_key("20260330T1200Z", "20260330T1500Z")
    )


# ── WRF ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_wrf_tile_falls_back_to_s3(mock_redis_client):
    mock_redis_client.get_wrf_tile = AsyncMock(return_value=None)
    mock_redis_client.store_wrf_tile = AsyncMock()  # for the cache-aside re-warm
    s3 = _s3_with_tile()
    strategy = WrfFullSyncStrategy(mock_redis_client, s3, 2592000, 2592000, 30)

    result = await strategy.get_tile(
        "Precipitacion1h", "20260603_060000", "F010", 5, 10, 15
    )

    assert result == TILE
    s3.download_tile.assert_awaited_once_with(
        S3Client.build_wrf_tile_key(
            "Precipitacion1h", "20260603_060000", "F010", 5, 10, 15
        )
    )


@pytest.mark.asyncio
async def test_wrf_tile_no_s3_returns_none(mock_redis_client):
    mock_redis_client.get_wrf_tile = AsyncMock(return_value=None)
    strategy = WrfFullSyncStrategy(mock_redis_client)  # Redis-only
    assert await strategy.get_tile("p", "i", "F001", 5, 0, 0) is None


@pytest.mark.asyncio
async def test_wrf_list_init_runs_falls_back_to_s3(mock_redis_client):
    mock_redis_client.get_wrf_init_runs = AsyncMock(return_value=[])
    s3 = _s3_listing(
        [
            "tiles/wrf/Precipitacion1h/20260603_000000/",
            "tiles/wrf/Precipitacion1h/20260603_060000/",
        ]
    )
    strategy = WrfFullSyncStrategy(mock_redis_client, s3, 2592000, 2592000, 30)

    # On-demand returns init runs sorted descending (newest first).
    assert await strategy.list_init_runs("Precipitacion1h") == [
        "20260603_060000",
        "20260603_000000",
    ]
