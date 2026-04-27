"""Integration tests for ECMWF mean sea level pressure endpoints."""

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from main import app
from models.ecmwf_mslp import (
    EcmwfMslpPointValueResponse,
    MslpForecastListResponse,
    MslpForecastRunInfo,
    MslpTimestampListResponse,
    TimestampInfo,
)
from services.ecmwf_mslp_service import EcmwfMslpService
from services.point_value_service import (
    CogNotFoundError,
    NoDataOrOutsideError,
    PointSample,
)

client = TestClient(app)

FORECAST_TS = "20260413T1200Z"
TIMESTAMP_TS = "20260413T1500Z"
BASE = "/products/ecmwf/mean-sea-level-pressure"


def _forecast_list():
    return MslpForecastListResponse(
        forecasts=[MslpForecastRunInfo(forecast_ts=FORECAST_TS, timestamp_count=47)]
    )


def _timestamp_list():
    return MslpTimestampListResponse(
        forecast_ts=FORECAST_TS,
        timestamps=[TimestampInfo(timestamp_ts=TIMESTAMP_TS)],
        bounding_box=EcmwfMslpService.BOUNDING_BOX,
    )


# ── GET /products/ecmwf/mean-sea-level-pressure ───────────────────────────────


def test_list_forecasts_returns_200():
    with patch("routes.ecmwf_mslp.ecmwf_mslp_service") as mock_svc:
        mock_svc.list_forecasts = AsyncMock(return_value=_forecast_list())

        response = client.get(BASE)

        assert response.status_code == 200
        data = response.json()
        assert len(data["forecasts"]) == 1
        assert data["forecasts"][0]["forecast_ts"] == FORECAST_TS
        assert data["forecasts"][0]["timestamp_count"] == 47


def test_list_forecasts_has_cache_headers():
    with patch("routes.ecmwf_mslp.ecmwf_mslp_service") as mock_svc:
        mock_svc.list_forecasts = AsyncMock(return_value=_forecast_list())

        response = client.get(BASE)

        assert "ETag" in response.headers
        assert "Cache-Control" in response.headers


def test_list_forecasts_304_on_etag_match():
    with patch("routes.ecmwf_mslp.ecmwf_mslp_service") as mock_svc:
        mock_svc.list_forecasts = AsyncMock(return_value=_forecast_list())

        first = client.get(BASE)
        etag = first.headers["ETag"]

        second = client.get(BASE, headers={"If-None-Match": etag})

        assert second.status_code == 304


# ── GET /products/ecmwf/mean-sea-level-pressure/{forecast_ts} ─────────────────


def test_list_timestamps_returns_200():
    with patch("routes.ecmwf_mslp.ecmwf_mslp_service") as mock_svc:
        mock_svc.list_timestamps = AsyncMock(return_value=_timestamp_list())

        response = client.get(f"{BASE}/{FORECAST_TS}")

        assert response.status_code == 200
        data = response.json()
        assert data["forecast_ts"] == FORECAST_TS
        assert len(data["timestamps"]) == 1
        assert data["timestamps"][0]["timestamp_ts"] == TIMESTAMP_TS
        assert "bounding_box" in data


def test_list_timestamps_returns_404_for_unknown_forecast():
    with patch("routes.ecmwf_mslp.ecmwf_mslp_service") as mock_svc:
        mock_svc.list_timestamps = AsyncMock(return_value=None)

        response = client.get(f"{BASE}/99991231T0000Z")

        assert response.status_code == 404


def test_list_timestamps_does_not_expose_zoom_or_url_pattern():
    """MSLP listing has no tiles, so it must omit url-pattern and zoom_levels."""
    with patch("routes.ecmwf_mslp.ecmwf_mslp_service") as mock_svc:
        mock_svc.list_timestamps = AsyncMock(return_value=_timestamp_list())

        data = client.get(f"{BASE}/{FORECAST_TS}").json()

        assert "tile_url_pattern" not in data
        assert "zoom_levels" not in data


# ── GET /products/ecmwf/mean-sea-level-pressure/{f}/{t}.json ──────────────────


def test_get_isobars_geojson_returns_200():
    geojson_bytes = b'{"type":"FeatureCollection","features":[]}'
    with patch("routes.ecmwf_mslp.ecmwf_mslp_service") as mock_svc:
        mock_svc.get_geojson = AsyncMock(return_value=geojson_bytes)

        response = client.get(f"{BASE}/{FORECAST_TS}/{TIMESTAMP_TS}.json")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/geo+json"
        assert response.content == geojson_bytes


def test_get_isobars_geojson_returns_404_when_missing():
    with patch("routes.ecmwf_mslp.ecmwf_mslp_service") as mock_svc:
        mock_svc.get_geojson = AsyncMock(return_value=None)

        response = client.get(f"{BASE}/{FORECAST_TS}/{TIMESTAMP_TS}.json")

        assert response.status_code == 404


def test_get_isobars_geojson_304_on_etag_match():
    geojson_bytes = b'{"type":"FeatureCollection","features":[]}'
    with patch("routes.ecmwf_mslp.ecmwf_mslp_service") as mock_svc:
        mock_svc.get_geojson = AsyncMock(return_value=geojson_bytes)

        first = client.get(f"{BASE}/{FORECAST_TS}/{TIMESTAMP_TS}.json")
        etag = first.headers["ETag"]

        second = client.get(
            f"{BASE}/{FORECAST_TS}/{TIMESTAMP_TS}.json",
            headers={"If-None-Match": etag},
        )

        assert second.status_code == 304


# ── GET /products/ecmwf/mean-sea-level-pressure/{f}/{t}/point ─────────────────


def test_get_point_value_returns_200():
    with patch("routes.ecmwf_mslp.point_value_service") as mock_pv:
        mock_pv.sample_ecmwf_mslp_point = AsyncMock(
            return_value=PointSample(value=1013.25, unit="hPa")
        )

        response = client.get(
            f"{BASE}/{FORECAST_TS}/{TIMESTAMP_TS}/point",
            params={"lat": -34.6, "lon": -58.4},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["forecast_ts"] == FORECAST_TS
        assert data["timestamp_ts"] == TIMESTAMP_TS
        assert data["lat"] == -34.6
        assert data["lon"] == -58.4
        assert data["value"] == 1013.25
        assert data["unit"] == "hPa"


def test_get_point_value_returns_404_when_cog_not_found():
    with patch("routes.ecmwf_mslp.point_value_service") as mock_pv:
        mock_pv.sample_ecmwf_mslp_point = AsyncMock(side_effect=CogNotFoundError())

        response = client.get(
            f"{BASE}/{FORECAST_TS}/{TIMESTAMP_TS}/point",
            params={"lat": -34.6, "lon": -58.4},
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "cog_not_found"


def test_get_point_value_returns_404_when_outside_or_nodata():
    with patch("routes.ecmwf_mslp.point_value_service") as mock_pv:
        mock_pv.sample_ecmwf_mslp_point = AsyncMock(side_effect=NoDataOrOutsideError())

        response = client.get(
            f"{BASE}/{FORECAST_TS}/{TIMESTAMP_TS}/point",
            params={"lat": -34.6, "lon": -58.4},
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "nodata_or_outside"


def test_get_point_value_validates_lat_range():
    with patch("routes.ecmwf_mslp.point_value_service") as mock_pv:
        mock_pv.sample_ecmwf_mslp_point = AsyncMock()
        response = client.get(
            f"{BASE}/{FORECAST_TS}/{TIMESTAMP_TS}/point",
            params={"lat": 91.0, "lon": -58.4},
        )
        assert response.status_code == 422


def test_get_point_value_validates_lon_range():
    with patch("routes.ecmwf_mslp.point_value_service") as mock_pv:
        mock_pv.sample_ecmwf_mslp_point = AsyncMock()
        response = client.get(
            f"{BASE}/{FORECAST_TS}/{TIMESTAMP_TS}/point",
            params={"lat": -34.6, "lon": -181.0},
        )
        assert response.status_code == 422
