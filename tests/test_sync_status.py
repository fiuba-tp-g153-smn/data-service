from fastapi.testclient import TestClient
from unittest.mock import AsyncMock
from main import app
from dependencies import get_redis_client


def test_sync_status_empty():
    """Every product reports defaults when no status is in Redis."""
    mock_redis = AsyncMock()
    mock_redis.get_domain_sync_status = AsyncMock(return_value={})

    app.dependency_overrides[get_redis_client] = lambda: mock_redis
    try:
        client = TestClient(app)
        response = client.get("/sync/status")

        assert response.status_code == 200
        data = response.json()
        assert data["any_running"] is False
        # One entry per product, all at defaults.
        domains = {d["domain"]: d for d in data["domains"]}
        assert set(domains) == {
            "satellite",
            "radar",
            "ecmwf_tp",
            "ecmwf_mslp",
            "wrf",
        }
        assert domains["satellite"]["total_cycles"] == 0
        assert domains["satellite"]["is_running"] is False
    finally:
        app.dependency_overrides.clear()


def test_sync_status_with_data():
    """Per-product status hashes are surfaced per domain, with an any_running rollup."""
    per_domain = {
        "satellite": {
            "is_running": "false",
            "last_sync_start": "1706000000.0",
            "last_sync_end": "1706000005.0",
            "last_sync_duration_ms": "5000",
            "last_sync_downloaded": "150",
            "last_sync_errors": "0",
            "consecutive_failures": "0",
            "total_cycles": "10",
        },
        "wrf": {"is_running": "true", "total_cycles": "3"},
    }

    mock_redis = AsyncMock()
    mock_redis.get_domain_sync_status = AsyncMock(
        side_effect=lambda domain: per_domain.get(domain, {})
    )

    app.dependency_overrides[get_redis_client] = lambda: mock_redis
    try:
        client = TestClient(app)
        response = client.get("/sync/status")

        assert response.status_code == 200
        data = response.json()
        # WRF is running -> rollup is true.
        assert data["any_running"] is True
        domains = {d["domain"]: d for d in data["domains"]}
        assert domains["satellite"]["total_cycles"] == 10
        assert domains["satellite"]["last_sync_duration_ms"] == 5000
        assert domains["satellite"]["last_sync_downloaded"] == 150
        assert domains["satellite"]["is_running"] is False
        assert domains["wrf"]["is_running"] is True
        assert domains["wrf"]["total_cycles"] == 3
    finally:
        app.dependency_overrides.clear()
