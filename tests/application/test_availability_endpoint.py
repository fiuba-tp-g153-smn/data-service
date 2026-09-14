"""Endpoint tests for the bundled /products/availability snapshot."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from services.product_availability_service import ProductAvailabilitySnapshot

URL = "/products/availability"


@pytest.fixture
def app_client(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    from main import app  # pylint: disable=import-outside-toplevel

    return TestClient(app)


@pytest.fixture
def service():
    with patch("routes.availability.product_availability_service") as mock:
        mock.snapshot = AsyncMock(
            return_value=ProductAvailabilitySnapshot(
                ["gfs/x", "radar-sinarame/RMA1/dbzh/elev0"],
                ["gfs", "radar-sinarame"],
            )
        )
        yield mock


def test_availability_is_not_swallowed_by_the_satellite_catch_all(app_client, service):
    """`/products/{product_id}` would match this URL if registered first."""
    response = app_client.get(URL)

    assert response.status_code == 200
    service.snapshot.assert_awaited_once()


def test_the_snapshot_is_returned_verbatim(app_client, service):
    body = app_client.get(URL).json()

    assert body["available"] == ["gfs/x", "radar-sinarame/RMA1/dbzh/elev0"]
    assert body["domains"] == ["gfs", "radar-sinarame"]


def test_an_unchanged_snapshot_revalidates_to_304(app_client, service):
    """The whole point: a client polling on a timer gets an empty body."""
    first = app_client.get(URL)
    etag = first.headers["etag"]

    second = app_client.get(URL, headers={"If-None-Match": etag})

    assert second.status_code == 304
    assert not second.content
    assert second.headers["cache-control"] == first.headers["cache-control"]


def test_a_changed_snapshot_gets_a_new_etag(app_client, service):
    etag = app_client.get(URL).headers["etag"]
    service.snapshot = AsyncMock(
        return_value=ProductAvailabilitySnapshot([], ["radar-sinarame"])
    )

    response = app_client.get(URL, headers={"If-None-Match": etag})

    assert response.status_code == 200
    assert response.headers["etag"] != etag
