"""API tests for point-value endpoints."""

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from main import app
from services.point_value_service import (
    CogNotFoundError,
    NoDataOrOutsideError,
    PointSample,
)

client = TestClient(app)


def test_satellite_point_endpoint_returns_value():
    with patch("routes.satellite.satellite_service") as mock_service:
        mock_service.channel_exists.return_value = True
        mock_service.get_point_value = AsyncMock(return_value=PointSample(291.1, "K"))

        response = client.get(
            "/products/goes-19/abi/ch-13/20260101T000000Z/point?lat=-34.6&lon=-58.4"
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["value"] == 291.1
        assert payload["unit"] == "K"


def test_satellite_point_endpoint_cog_not_found():
    with patch("routes.satellite.satellite_service") as mock_service:
        mock_service.channel_exists.return_value = True
        mock_service.get_point_value = AsyncMock(side_effect=CogNotFoundError())

        response = client.get(
            "/products/goes-19/abi/ch-13/unknown/point?lat=-34.6&lon=-58.4"
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "cog_not_found"


def test_satellite_point_endpoint_invalid_latlon_422():
    with patch("routes.satellite.satellite_service") as mock_service:
        mock_service.channel_exists.return_value = True

        response = client.get(
            "/products/goes-19/abi/ch-13/20260101T000000Z/point?lat=-95.0&lon=-58.4"
        )

        assert response.status_code == 422


def test_radar_point_endpoint_nodata_or_outside():
    with patch("routes.radar.radar_service") as mock_service:
        mock_service.get_point_value = AsyncMock(side_effect=NoDataOrOutsideError())

        response = client.get(
            "/products/radar/RMA1/DBZH/elev0/20260114T170328Z/point?lat=-34.6&lon=-58.4"
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "nodata_or_outside"
