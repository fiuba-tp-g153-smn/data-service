"""Shared test fixtures for the data-service test suite."""

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def mock_redis_client():
    """Shared mock for RedisClient used across tests."""
    client = MagicMock()
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.health_check = AsyncMock(return_value=True)

    # Satellite tile operations
    client.store_satellite_tile = AsyncMock()
    client.get_satellite_tile = AsyncMock(return_value=None)

    # Satellite index operations
    client.add_satellite_tileset = AsyncMock()
    client.get_satellite_tilesets = AsyncMock(return_value=[])
    client.delete_satellite_tileset = AsyncMock()
    client.satellite_tileset_exists = AsyncMock(return_value=False)

    # Radar tile operations
    client.store_radar_tile = AsyncMock()
    client.get_radar_tile = AsyncMock(return_value=None)

    # Radar index operations
    client.add_radar_index = AsyncMock()
    client.get_radar_radars = AsyncMock(return_value=[])
    client.get_radar_variables = AsyncMock(return_value=[])
    client.get_radar_elevations = AsyncMock(return_value=[])
    client.get_radar_tilesets = AsyncMock(return_value=[])

    # Sync status operations
    client.update_sync_status = AsyncMock()
    client.get_sync_status = AsyncMock(return_value={})

    # ECMWF total precipitation tile operations
    client.store_ecmwf_tp_tile = AsyncMock()
    client.get_ecmwf_tp_tile = AsyncMock(return_value=None)

    # ECMWF total precipitation index operations
    client.store_ecmwf_tp_index = AsyncMock()
    client.get_ecmwf_tp_forecasts = AsyncMock(return_value=[])
    client.get_ecmwf_tp_periods = AsyncMock(return_value=[])

    # ECMWF MSLP GeoJSON operations
    client.store_ecmwf_mslp_geojson = AsyncMock()
    client.get_ecmwf_mslp_geojson = AsyncMock(return_value=None)

    # ECMWF MSLP index operations
    client.store_ecmwf_mslp_index = AsyncMock()
    client.get_ecmwf_mslp_forecasts = AsyncMock(return_value=[])
    client.get_ecmwf_mslp_timestamps = AsyncMock(return_value=[])

    # Listing cache (shared across sources)
    client.get_cached_listing = AsyncMock(return_value=None)
    client.cache_listing = AsyncMock()

    return client
