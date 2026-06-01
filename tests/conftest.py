"""Shared test fixtures for the data-service test suite."""

import os

# Settings._validate() runs at module-import time via `dependencies.py`'s
# top-level `Settings.get_settings()` call. Several validation rules require
# env vars that operators normally set in `.env` (weather-stations admin
# password, SMN credentials). Without these, ANY test module that imports
# `main`, `dependencies`, or any service that transitively pulls in
# `dependencies` fails to collect — see CI failures like
# "weather_stations_api_key_auth_enabled=true requires WEATHER_STATIONS_ADMIN_PASSWORD".
#
# Set safe placeholder values BEFORE any test module is loaded. `setdefault`
# preserves real values when present (local dev with actual SMN creds keeps
# working).
os.environ.setdefault("WEATHER_STATIONS_ADMIN_PASSWORD", "test-admin-password")
os.environ.setdefault("SMN_API_USERNAME", "test-user")
os.environ.setdefault("SMN_API_PASSWORD", "test-pass")
# The keystore now persists to S3, and Settings._validate() refuses to start
# with auth enabled unless S3 is configured. Tests never touch a real S3 —
# placeholders satisfy `is_s3_configured()` so the validator passes.
os.environ.setdefault("S3_TILES_DATA_ENDPOINT", "test-endpoint:9000")
os.environ.setdefault("S3_TILES_DATA_ACCESS_KEY", "test-access-key")
os.environ.setdefault("S3_TILES_DATA_SECRET_KEY", "test-secret-key")

from unittest.mock import AsyncMock, MagicMock  # noqa: E402

import pytest  # noqa: E402


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
    client.trim_satellite_index = AsyncMock(return_value=0)
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
    client.trim_radar_index = AsyncMock(return_value=0)

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
    client.prune_ecmwf_tp_forecasts = AsyncMock(return_value=0)

    # ECMWF MSLP GeoJSON operations
    client.store_ecmwf_mslp_geojson = AsyncMock()
    client.get_ecmwf_mslp_geojson = AsyncMock(return_value=None)

    # ECMWF MSLP index operations
    client.store_ecmwf_mslp_index = AsyncMock()
    client.get_ecmwf_mslp_forecasts = AsyncMock(return_value=[])
    client.get_ecmwf_mslp_timestamps = AsyncMock(return_value=[])
    client.prune_ecmwf_mslp_forecasts = AsyncMock(return_value=0)

    # Listing cache (shared across sources)
    client.get_cached_listing = AsyncMock(return_value=None)
    client.cache_listing = AsyncMock()

    return client
