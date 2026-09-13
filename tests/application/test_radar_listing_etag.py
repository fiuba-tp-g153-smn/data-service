"""The radar tileset listing must revalidate, not resend.

It is the most-polled listing in the service — the frontend has 18 radars x 6
variables — and it used to answer a full body every time because, unlike the
WRF and GFS listings, it carried no ETag at all.
"""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

URL = "/products/radar-sinarame/RMA1/dbzh/elev0"


@pytest.fixture
def app_client(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    from main import app  # pylint: disable=import-outside-toplevel

    return TestClient(app)


@pytest.fixture
def service():
    with patch("routes.radar.radar_service") as mock:
        mock.list_radar_tilesets = AsyncMock(
            return_value={
                "radar": "RMA1",
                "variable": "dbzh",
                "elevation": "elev0",
                "tilesets": ["20260913T150000Z"],
            }
        )
        yield mock


def test_the_listing_keeps_its_shape(app_client, service):
    """The frontend reads `tilesets`; the response_model must not reshape it."""
    body = app_client.get(URL).json()

    assert body == {
        "radar": "RMA1",
        "variable": "dbzh",
        "elevation": "elev0",
        "tilesets": ["20260913T150000Z"],
    }


def test_an_unchanged_listing_revalidates_to_304(app_client, service):
    first = app_client.get(URL)
    assert first.headers["cache-control"]

    second = app_client.get(URL, headers={"If-None-Match": first.headers["etag"]})

    assert second.status_code == 304
    assert not second.content


def test_a_new_tileset_invalidates_the_etag(app_client, service):
    """A gap must never keep serving 304 once the data lands."""
    etag = app_client.get(URL).headers["etag"]
    service.list_radar_tilesets = AsyncMock(
        return_value={
            "radar": "RMA1",
            "variable": "dbzh",
            "elevation": "elev0",
            "tilesets": ["20260913T150000Z", "20260913T151000Z"],
        }
    )

    response = app_client.get(URL, headers={"If-None-Match": etag})

    assert response.status_code == 200
    assert len(response.json()["tilesets"]) == 2
