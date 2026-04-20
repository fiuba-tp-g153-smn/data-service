"""Route-level tests for /basemap/{provider_id}/{z}/{x}/{y}.png."""

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def init_env_vars(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "INFO")


@pytest.fixture
def app_with_basemap_stub(init_env_vars):
    """TestClient with `get_basemap_service` overridden to a scriptable stub."""
    from dependencies import get_basemap_service
    from main import app

    stub = AsyncMock()
    stub.validate_provider = lambda pid: pid == "argenmap"
    stub.get_tile_data = AsyncMock(return_value=None)

    app.dependency_overrides[get_basemap_service] = lambda: stub
    try:
        yield TestClient(app), stub
    finally:
        app.dependency_overrides.pop(get_basemap_service, None)


def test_z2_returns_transparent_png_when_tile_missing(app_with_basemap_stub):
    """z=2 used to 400. Now the reader returns None → route serves a transparent PNG."""
    client, stub = app_with_basemap_stub
    stub.get_tile_data = AsyncMock(return_value=None)

    response = client.get("/basemap/argenmap/2/1/1.png")

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    # Body is the cached transparent PNG singleton.
    from routes.utils import TRANSPARENT_PNG_TILE

    assert response.content == TRANSPARENT_PNG_TILE
    stub.get_tile_data.assert_awaited_once_with("argenmap", 2, 1, 1)


def test_miss_carries_miss_cache_control(app_with_basemap_stub):
    """Missing tiles must set the miss-specific Cache-Control so browsers stop refetching."""
    client, stub = app_with_basemap_stub
    stub.get_tile_data = AsyncMock(return_value=None)

    from dependencies import settings

    response = client.get("/basemap/argenmap/4/5/9.png")
    assert response.status_code == 200
    assert response.headers["cache-control"] == settings.basemap_cache_control_tile_miss


def test_z2_returns_tile_when_relay_has_it(app_with_basemap_stub):
    client, stub = app_with_basemap_stub
    stub.get_tile_data = AsyncMock(return_value=b"\x89PNG\r\n\x1a\n")

    response = client.get("/basemap/argenmap/2/1/1.png")

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"


def test_hit_uses_basemap_specific_cache_control(app_with_basemap_stub):
    """Basemap hits must use the basemap-specific Cache-Control (1 week),
    not the shared satellite/radar one (12 h)."""
    client, stub = app_with_basemap_stub
    stub.get_tile_data = AsyncMock(return_value=b"\x89PNG\r\n\x1a\n")

    from dependencies import settings

    response = client.get("/basemap/argenmap/4/5/9.png")
    assert response.status_code == 200
    assert response.headers["cache-control"] == settings.basemap_cache_control_tile
    # Sanity: the default is 1 week. Config may override, but the shipped
    # default must not regress to the shared 12h header by accident.
    assert "max-age=604800" in settings.basemap_cache_control_tile


def test_unknown_provider_returns_404(app_with_basemap_stub):
    client, _ = app_with_basemap_stub
    response = client.get("/basemap/no-such-provider/4/5/9.png")
    assert response.status_code == 404


def test_zoom_above_envelope_returns_422(app_with_basemap_stub):
    """FastAPI path validator (le=22) rejects out-of-envelope zooms."""
    client, _ = app_with_basemap_stub
    response = client.get("/basemap/argenmap/25/1/1.png")
    assert response.status_code == 422


def test_negative_zoom_returns_422(app_with_basemap_stub):
    client, _ = app_with_basemap_stub
    response = client.get("/basemap/argenmap/-1/0/0.png")
    assert response.status_code == 422
