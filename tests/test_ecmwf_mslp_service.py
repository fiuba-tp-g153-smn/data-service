"""Unit tests for EcmwfMslpService."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.ecmwf_mslp_service import EcmwfMslpService

FORECAST_TS = "20260413T1200Z"
TIMESTAMP_TS = "20260413T1500Z"
ALL_FORECASTS = [FORECAST_TS, "20260413T0000Z", "20260412T1200Z"]


def _make_strategy(forecasts=None, timestamps=None, geojson=None):
    strategy = MagicMock()
    strategy.list_forecasts = AsyncMock(return_value=forecasts or [])
    strategy.list_timestamps = AsyncMock(return_value=timestamps or [])
    strategy.get_geojson = AsyncMock(return_value=geojson)
    return strategy


@pytest.mark.asyncio
async def test_list_forecasts_returns_empty_when_no_strategy():
    service = EcmwfMslpService()
    result = await service.list_forecasts()
    assert result.forecasts == []


@pytest.mark.asyncio
async def test_list_forecasts_limits_by_forecasts_to_keep():
    service = EcmwfMslpService()
    strategy = _make_strategy(forecasts=ALL_FORECASTS, timestamps=[TIMESTAMP_TS])
    service.set_strategy(strategy)

    with patch("services.ecmwf_mslp_service.settings") as mock_settings:
        mock_settings.ecmwf_forecasts_to_keep = 2
        result = await service.list_forecasts()

    assert len(result.forecasts) == 2
    assert [f.forecast_ts for f in result.forecasts] == ALL_FORECASTS[:2]
    assert all(f.timestamp_count == 1 for f in result.forecasts)


@pytest.mark.asyncio
async def test_list_timestamps_returns_none_when_no_strategy():
    service = EcmwfMslpService()
    result = await service.list_timestamps(FORECAST_TS)
    assert result is None


@pytest.mark.asyncio
async def test_list_timestamps_returns_none_for_inactive_forecast():
    service = EcmwfMslpService()
    strategy = _make_strategy(forecasts=ALL_FORECASTS, timestamps=[TIMESTAMP_TS])
    service.set_strategy(strategy)

    with patch("services.ecmwf_mslp_service.settings") as mock_settings:
        mock_settings.ecmwf_forecasts_to_keep = 1
        # ALL_FORECASTS[1] is the second-most-recent, not active when keep=1
        result = await service.list_timestamps(ALL_FORECASTS[1])

    assert result is None


@pytest.mark.asyncio
async def test_list_timestamps_returns_full_response():
    service = EcmwfMslpService()
    strategy = _make_strategy(
        forecasts=ALL_FORECASTS, timestamps=[TIMESTAMP_TS, "20260413T1800Z"]
    )
    service.set_strategy(strategy)

    with patch("services.ecmwf_mslp_service.settings") as mock_settings:
        mock_settings.ecmwf_forecasts_to_keep = 2
        result = await service.list_timestamps(FORECAST_TS)

    assert result is not None
    assert result.forecast_ts == FORECAST_TS
    assert [t.timestamp_ts for t in result.timestamps] == [
        TIMESTAMP_TS,
        "20260413T1800Z",
    ]
    assert result.bounding_box == EcmwfMslpService.BOUNDING_BOX


@pytest.mark.asyncio
async def test_get_geojson_returns_none_when_no_strategy():
    service = EcmwfMslpService()
    result = await service.get_geojson(FORECAST_TS, TIMESTAMP_TS)
    assert result is None


@pytest.mark.asyncio
async def test_get_geojson_delegates_to_strategy():
    service = EcmwfMslpService()
    strategy = _make_strategy(geojson=b'{"type":"FeatureCollection"}')
    service.set_strategy(strategy)

    result = await service.get_geojson(FORECAST_TS, TIMESTAMP_TS)
    assert result == b'{"type":"FeatureCollection"}'
    strategy.get_geojson.assert_awaited_once_with(FORECAST_TS, TIMESTAMP_TS)
