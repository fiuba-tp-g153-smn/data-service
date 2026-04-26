"""Unit tests for EcmwfService."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.ecmwf_service import EcmwfService

FORECAST_TS = "20260330T1200Z"
PERIOD_TS = "20260330T1500Z-20260330T1800Z"
ALL_FORECASTS = [FORECAST_TS, "20260330T0000Z", "20260329T1200Z"]


def _make_strategy(forecasts=None, periods=None, tile=None):
    strategy = MagicMock()
    strategy.list_forecasts = AsyncMock(return_value=forecasts or [])
    strategy.list_periods = AsyncMock(return_value=periods or [])
    strategy.get_tile = AsyncMock(return_value=tile)
    return strategy


# ── list_forecasts ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_forecasts_no_strategy_returns_empty():
    service = EcmwfService()

    result = await service.list_forecasts()

    assert result.forecasts == []


@pytest.mark.asyncio
async def test_list_forecasts_limited_to_forecasts_to_keep():
    service = EcmwfService()
    periods = [f"p{i}" for i in range(48)]
    strategy = _make_strategy(forecasts=ALL_FORECASTS, periods=periods)
    service.set_strategy(strategy)

    with patch("services.ecmwf_service.settings") as mock_settings:
        mock_settings.ecmwf_forecasts_to_keep = 2
        result = await service.list_forecasts()

    assert len(result.forecasts) == 2
    assert result.forecasts[0].forecast_ts == FORECAST_TS
    assert result.forecasts[1].forecast_ts == "20260330T0000Z"


@pytest.mark.asyncio
async def test_list_forecasts_includes_period_count():
    service = EcmwfService()
    strategy = _make_strategy(forecasts=[FORECAST_TS], periods=[PERIOD_TS])
    service.set_strategy(strategy)

    with patch("services.ecmwf_service.settings") as mock_settings:
        mock_settings.ecmwf_forecasts_to_keep = 2
        result = await service.list_forecasts()

    assert result.forecasts[0].period_count == 1


# ── list_periods ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_periods_returns_none_for_unknown_forecast():
    service = EcmwfService()
    strategy = _make_strategy(forecasts=[FORECAST_TS])
    service.set_strategy(strategy)

    with patch("services.ecmwf_service.settings") as mock_settings:
        mock_settings.ecmwf_forecasts_to_keep = 2
        result = await service.list_periods("99991231T0000Z")

    assert result is None


@pytest.mark.asyncio
async def test_list_periods_returns_none_when_forecast_beyond_limit():
    service = EcmwfService()
    strategy = _make_strategy(forecasts=ALL_FORECASTS)
    service.set_strategy(strategy)

    with patch("services.ecmwf_service.settings") as mock_settings:
        mock_settings.ecmwf_forecasts_to_keep = 2
        # "20260329T1200Z" is 3rd — beyond the keep limit of 2
        result = await service.list_periods("20260329T1200Z")

    assert result is None


@pytest.mark.asyncio
async def test_list_periods_returns_response_with_periods():
    service = EcmwfService()
    strategy = _make_strategy(forecasts=[FORECAST_TS], periods=[PERIOD_TS])
    service.set_strategy(strategy)

    with patch("services.ecmwf_service.settings") as mock_settings:
        mock_settings.ecmwf_forecasts_to_keep = 2
        result = await service.list_periods(FORECAST_TS)

    assert result is not None
    assert result.forecast_ts == FORECAST_TS
    assert len(result.periods) == 1
    assert result.periods[0].period_ts == PERIOD_TS


@pytest.mark.asyncio
async def test_list_periods_response_includes_tile_url_pattern():
    service = EcmwfService()
    strategy = _make_strategy(forecasts=[FORECAST_TS], periods=[PERIOD_TS])
    service.set_strategy(strategy)

    with patch("services.ecmwf_service.settings") as mock_settings:
        mock_settings.ecmwf_forecasts_to_keep = 2
        result = await service.list_periods(FORECAST_TS)

    assert "{forecast_ts}" in result.tile_url_pattern
    assert "{period_ts}" in result.tile_url_pattern


@pytest.mark.asyncio
async def test_list_periods_no_strategy_returns_none():
    service = EcmwfService()

    result = await service.list_periods(FORECAST_TS)

    assert result is None


# ── get_tile_data ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_tile_data_delegates_to_strategy():
    service = EcmwfService()
    strategy = _make_strategy(tile=b"webp-bytes")
    service.set_strategy(strategy)

    result = await service.get_tile_data(FORECAST_TS, PERIOD_TS, 5, 10, 15)

    assert result == b"webp-bytes"
    strategy.get_tile.assert_awaited_once_with(FORECAST_TS, PERIOD_TS, 5, 10, 15)


@pytest.mark.asyncio
async def test_get_tile_data_returns_none_on_miss():
    service = EcmwfService()
    strategy = _make_strategy(tile=None)
    service.set_strategy(strategy)

    result = await service.get_tile_data(FORECAST_TS, PERIOD_TS, 5, 0, 0)

    assert result is None


@pytest.mark.asyncio
async def test_get_tile_data_no_strategy_returns_none():
    service = EcmwfService()

    result = await service.get_tile_data(FORECAST_TS, PERIOD_TS, 5, 0, 0)

    assert result is None
