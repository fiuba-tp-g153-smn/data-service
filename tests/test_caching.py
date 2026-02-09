from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
from main import app
from datetime import datetime, timezone
from dependencies import settings

client = TestClient(app)


def test_get_channel_config_headers():
    """Verify that channel config endpoint returns correct caching headers."""
    mock_data = {
        "tilesets": [{"id": "t1_s20260101000000"}],
        "channel_info": {},
        "tile_url_pattern": "pattern",
    }

    with patch("routes.satellite.satellite_service") as mock_service:
        mock_service.channel_exists.return_value = True
        mock_service.get_channel_tilesets = AsyncMock(return_value=mock_data)
        mock_service.get_config_hash.return_value = "test-hash-123"
        mock_service.get_latest_tileset_timestamp.return_value = datetime(
            2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc
        )

        response = client.get("/products/goes-19/abi/ch-13")

        assert response.status_code == 200
        assert response.headers["Cache-Control"] == settings.cache_control_config
        assert response.headers["ETag"] == '"test-hash-123"'
        assert response.headers["Last-Modified"] == "Thu, 01 Jan 2026 12:00:00 GMT"


def test_get_tile_headers():
    """Verify that tile endpoint returns correct caching headers (Immutable, ETag)."""
    tile_bytes = b"\x00\x01\x02\x03"

    with patch("routes.satellite.satellite_service") as mock_service:
        mock_service.channel_exists.return_value = True
        mock_service.validate_zoom_level.return_value = (True, "")
        mock_service.get_tile_data = AsyncMock(return_value=tile_bytes)

        response = client.get("/products/goes-19/abi/ch-13/t1/5/1/1.webp")

        assert response.status_code == 200
        assert "immutable" in response.headers["Cache-Control"]
        assert response.headers["Cache-Control"] == settings.cache_control_tile
        assert response.headers["ETag"] == '"t1-5-1-1"'


def test_tile_304_not_modified():
    """Verify that tile endpoint returns 304 when If-None-Match matches ETag."""
    with patch("routes.satellite.satellite_service") as mock_service:
        mock_service.channel_exists.return_value = True
        mock_service.validate_zoom_level.return_value = (True, "")

        response = client.get(
            "/products/goes-19/abi/ch-13/t1/5/1/1.webp",
            headers={"If-None-Match": '"t1-5-1-1"'},
        )

        assert response.status_code == 304
        # get_tile_data should not have been called
        mock_service.get_tile_data.assert_not_called()


def test_config_304_not_modified():
    """Verify that config endpoint returns 304 when If-None-Match matches ETag."""
    mock_data = {
        "tilesets": [{"id": "t1_s20260101000000"}],
        "channel_info": {},
        "tile_url_pattern": "pattern",
    }

    with patch("routes.satellite.satellite_service") as mock_service:
        mock_service.channel_exists.return_value = True
        mock_service.get_channel_tilesets = AsyncMock(return_value=mock_data)
        mock_service.get_config_hash.return_value = "test-hash-123"
        mock_service.get_latest_tileset_timestamp.return_value = None

        # First request to get the ETag
        response = client.get(
            "/products/goes-19/abi/ch-13",
            headers={"If-None-Match": '"test-hash-123"'},
        )

        assert response.status_code == 304
