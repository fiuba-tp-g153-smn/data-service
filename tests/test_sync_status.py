from fastapi.testclient import TestClient
from unittest.mock import AsyncMock
from main import app
from dependencies import get_redis_client


def test_sync_status_empty():
    """Verify sync status returns defaults when no data in Redis."""
    mock_redis = AsyncMock()
    mock_redis.get_sync_status = AsyncMock(return_value={})

    app.dependency_overrides[get_redis_client] = lambda: mock_redis
    try:
        client = TestClient(app)
        response = client.get("/sync/status")

        assert response.status_code == 200
        data = response.json()
        assert data["is_running"] is False
        assert data["total_cycles"] == 0
        assert data["satellite_tilesets_count"] == 0
        assert data["radar_tilesets_count"] == 0
    finally:
        app.dependency_overrides.clear()


def test_sync_status_with_data():
    """Verify sync status returns data from Redis."""
    mock_redis = AsyncMock()
    mock_redis.get_sync_status = AsyncMock(
        return_value={
            "is_running": "false",
            "last_sync_start": "1706000000.0",
            "last_sync_end": "1706000005.0",
            "last_sync_duration_ms": "5000",
            "last_sync_downloaded": "150",
            "last_sync_errors": "0",
            "consecutive_failures": "0",
            "total_cycles": "10",
            "satellite_tilesets_count": "78",
            "radar_tilesets_count": "12",
        }
    )

    app.dependency_overrides[get_redis_client] = lambda: mock_redis
    try:
        client = TestClient(app)
        response = client.get("/sync/status")

        assert response.status_code == 200
        data = response.json()
        assert data["is_running"] is False
        assert data["total_cycles"] == 10
        assert data["satellite_tilesets_count"] == 78
        assert data["radar_tilesets_count"] == 12
        assert data["last_sync_duration_ms"] == 5000
        assert data["last_sync_downloaded"] == 150
    finally:
        app.dependency_overrides.clear()
