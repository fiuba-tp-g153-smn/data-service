"""Integration tests for ECMWF total precipitation endpoints."""

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from main import app
from models.ecmwf import (
    ForecastListResponse,
    ForecastRunInfo,
    PeriodInfo,
    PeriodListResponse,
)
from services.ecmwf_service import EcmwfService
from services.point_value_service import (
    CogNotFoundError,
    NoDataOrOutsideError,
    PointSample,
)

client = TestClient(app)

FORECAST_TS = "20260330T1200Z"
PERIOD_TS = "20260330T1500Z-20260330T1800Z"
BASE = "/products/ecmwf/total-precipitation"


def _forecast_list():
    return ForecastListResponse(
        forecasts=[ForecastRunInfo(forecast_ts=FORECAST_TS, period_count=48)]
    )


def _period_list():
    return PeriodListResponse(
        forecast_ts=FORECAST_TS,
        periods=[PeriodInfo(period_ts=PERIOD_TS)],
        tile_url_pattern=EcmwfService.TILE_URL_PATTERN,
        zoom_levels=EcmwfService.ZOOM_LEVELS,
        bounding_box=EcmwfService.BOUNDING_BOX,
    )


# ── GET /products/ecmwf/total-precipitation ───────────────────────────────────


def test_list_forecasts_returns_200():
    with patch("routes.ecmwf.ecmwf_service") as mock_svc:
        mock_svc.list_forecasts = AsyncMock(return_value=_forecast_list())

        response = client.get(BASE)

        assert response.status_code == 200
        data = response.json()
        assert len(data["forecasts"]) == 1
        assert data["forecasts"][0]["forecast_ts"] == FORECAST_TS
        assert data["forecasts"][0]["period_count"] == 48


def test_list_forecasts_has_cache_headers():
    with patch("routes.ecmwf.ecmwf_service") as mock_svc:
        mock_svc.list_forecasts = AsyncMock(return_value=_forecast_list())

        response = client.get(BASE)

        assert "ETag" in response.headers
        assert "Cache-Control" in response.headers


def test_list_forecasts_304_on_etag_match():
    with patch("routes.ecmwf.ecmwf_service") as mock_svc:
        mock_svc.list_forecasts = AsyncMock(return_value=_forecast_list())

        first = client.get(BASE)
        etag = first.headers["ETag"]

        second = client.get(BASE, headers={"If-None-Match": etag})

        assert second.status_code == 304


def test_list_forecasts_200_on_etag_mismatch():
    with patch("routes.ecmwf.ecmwf_service") as mock_svc:
        mock_svc.list_forecasts = AsyncMock(return_value=_forecast_list())

        response = client.get(BASE, headers={"If-None-Match": '"stale-etag"'})

        assert response.status_code == 200


# ── GET /products/ecmwf/total-precipitation/{forecast_ts} ─────────────────────


def test_list_periods_returns_200():
    with patch("routes.ecmwf.ecmwf_service") as mock_svc:
        mock_svc.list_periods = AsyncMock(return_value=_period_list())

        response = client.get(f"{BASE}/{FORECAST_TS}")

        assert response.status_code == 200
        data = response.json()
        assert data["forecast_ts"] == FORECAST_TS
        assert len(data["periods"]) == 1
        assert data["periods"][0]["period_ts"] == PERIOD_TS


def test_list_periods_returns_404_for_unknown_forecast():
    with patch("routes.ecmwf.ecmwf_service") as mock_svc:
        mock_svc.list_periods = AsyncMock(return_value=None)

        response = client.get(f"{BASE}/99991231T0000Z")

        assert response.status_code == 404


def test_list_periods_304_on_etag_match():
    with patch("routes.ecmwf.ecmwf_service") as mock_svc:
        mock_svc.list_periods = AsyncMock(return_value=_period_list())

        first = client.get(f"{BASE}/{FORECAST_TS}")
        etag = first.headers["ETag"]

        second = client.get(f"{BASE}/{FORECAST_TS}", headers={"If-None-Match": etag})

        assert second.status_code == 304


# ── GET /products/ecmwf/total-precipitation/{f}/{p}/{z}/{x}/{y}.webp ──────────


def test_get_tile_returns_200_webp():
    with patch("routes.ecmwf.ecmwf_service") as mock_svc:
        mock_svc.get_tile_data = AsyncMock(return_value=b"\x52\x49\x46\x46")

        response = client.get(f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/5/10/15.webp")

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "image/webp"


def test_get_tile_has_immutable_cache_header():
    with patch("routes.ecmwf.ecmwf_service") as mock_svc:
        mock_svc.get_tile_data = AsyncMock(return_value=b"\x00")

        response = client.get(f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/5/0/0.webp")

        assert "immutable" in response.headers["Cache-Control"]


def test_get_tile_returns_404_when_not_found():
    with patch("routes.ecmwf.ecmwf_service") as mock_svc:
        mock_svc.get_tile_data = AsyncMock(return_value=None)

        response = client.get(f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/5/0/0.webp")

        assert response.status_code == 404


def test_get_tile_returns_400_for_zoom_too_high():
    response = client.get(f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/10/0/0.webp")

    assert response.status_code == 400


def test_get_tile_returns_400_for_zoom_too_low():
    response = client.get(f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/1/0/0.webp")

    assert response.status_code == 400


def test_get_tile_returns_304_on_etag_match():
    etag = f'"{FORECAST_TS}-{PERIOD_TS}-5-10-15"'

    response = client.get(
        f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/5/10/15.webp",
        headers={"If-None-Match": etag},
    )

    assert response.status_code == 304


# ── GET /products/ecmwf/total-precipitation/{f}/{p}/point ─────────────────────


def test_get_point_value_returns_200():
    with patch("routes.ecmwf.point_value_service") as mock_pv:
        mock_pv.sample_ecmwf_point = AsyncMock(
            return_value=PointSample(value=12.5, unit="mm")
        )

        response = client.get(
            f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/point?lat=-34.6&lon=-58.4"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["value"] == 12.5
        assert data["unit"] == "mm"
        assert data["forecast_ts"] == FORECAST_TS
        assert data["period_ts"] == PERIOD_TS
        assert data["lat"] == -34.6
        assert data["lon"] == -58.4


def test_get_point_value_returns_404_cog_not_found():
    with patch("routes.ecmwf.point_value_service") as mock_pv:
        mock_pv.sample_ecmwf_point = AsyncMock(side_effect=CogNotFoundError())

        response = client.get(
            f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/point?lat=-34.6&lon=-58.4"
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "cog_not_found"


def test_get_point_value_returns_404_nodata_or_outside():
    with patch("routes.ecmwf.point_value_service") as mock_pv:
        mock_pv.sample_ecmwf_point = AsyncMock(side_effect=NoDataOrOutsideError())

        response = client.get(
            f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/point?lat=-34.6&lon=-58.4"
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "nodata_or_outside"


def test_get_point_value_returns_422_for_lat_out_of_range():
    response = client.get(f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/point?lat=-95.0&lon=-58.4")

    assert response.status_code == 422


def test_get_point_value_returns_422_for_lon_out_of_range():
    response = client.get(f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/point?lat=-34.6&lon=200.0")

    assert response.status_code == 422


def test_get_point_value_returns_422_when_missing_params():
    response = client.get(f"{BASE}/{FORECAST_TS}/{PERIOD_TS}/point")

    assert response.status_code == 422
