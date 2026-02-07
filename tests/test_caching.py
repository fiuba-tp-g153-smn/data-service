from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, patch
from main import app
from datetime import datetime, timezone
import pytest
from pathlib import Path

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
        assert (
            response.headers["Cache-Control"]
            == "public, max-age=60, stale-while-revalidate=300"
        )
        assert response.headers["ETag"] == '"test-hash-123"'
        assert response.headers["Last-Modified"] == "Thu, 01 Jan 2026 12:00:00 GMT"


def test_get_tile_headers(tmp_path):
    """Verify that tile endpoint returns correct caching headers (Immutable, ETag)."""
    # Create dummy tile file
    tile_file = tmp_path / "test_tile.webp"
    tile_file.touch()

    with patch("routes.satellite.satellite_service") as mock_service:
        mock_service.channel_exists.return_value = True
        mock_service.validate_zoom_level.return_value = (True, "")
        mock_service.get_tile_path.return_value = tile_file

        response = client.get("/products/goes-19/abi/ch-13/t1/1/1/1.webp")

        assert response.status_code == 200
        assert "immutable" in response.headers["Cache-Control"]
        assert (
            response.headers["Cache-Control"] == "public, max-age=31536000, immutable"
        )
        assert response.headers["ETag"] == '"t1-1-1-1"'
