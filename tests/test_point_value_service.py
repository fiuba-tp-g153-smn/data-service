"""Unit tests for point-value COG sampling service."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from services.point_value_service import (
    CogNotFoundError,
    NoDataOrOutsideError,
    PointValueService,
)
from services.point_value_strategy import (
    CogObjectNotFoundError,
    NoDataOrOutsideSampleError,
)


@pytest.mark.asyncio
async def test_sample_satellite_point_uses_unit():
    service = PointValueService()
    strategy = MagicMock()
    strategy.sample_cog_value = AsyncMock(return_value=123.4)
    service.set_strategy(strategy)

    sample = await service.sample_satellite_point("band_13", "t1", -34.0, -58.0)

    assert sample.value == 123.4
    assert sample.unit == "K"


@pytest.mark.asyncio
async def test_sample_satellite_point_uses_unit_for_glm_band():
    service = PointValueService()
    strategy = MagicMock()
    strategy.sample_cog_value = AsyncMock(return_value=123.4)
    service.set_strategy(strategy)

    sample = await service.sample_satellite_point("glm_fed", "t1", -34.0, -58.0)

    assert sample.value == 123.4
    assert sample.unit == "flashes/min"


@pytest.mark.asyncio
async def test_sample_raises_cog_not_found():
    service = PointValueService()
    strategy = MagicMock()
    strategy.sample_cog_value = AsyncMock(side_effect=CogObjectNotFoundError())
    service.set_strategy(strategy)

    with pytest.raises(CogNotFoundError):
        await service.sample_satellite_point("band_13", "missing", -34.0, -58.0)


@pytest.mark.asyncio
async def test_sample_raises_no_data_or_outside():
    service = PointValueService()
    strategy = MagicMock()
    strategy.sample_cog_value = AsyncMock(side_effect=NoDataOrOutsideSampleError())
    service.set_strategy(strategy)

    with pytest.raises(NoDataOrOutsideError):
        await service.sample_satellite_point("band_13", "t1", -34.0, -58.0)


@pytest.mark.asyncio
async def test_sample_ecmwf_tp_point_returns_mm_unit():
    service = PointValueService()
    strategy = MagicMock()
    strategy.sample_cog_value = AsyncMock(return_value=5.2)
    service.set_strategy(strategy)

    sample = await service.sample_ecmwf_tp_point(
        "20260330T1200Z", "20260330T1500Z", -34.0, -58.0
    )

    assert sample.value == 5.2
    assert sample.unit == "mm"


@pytest.mark.asyncio
async def test_sample_ecmwf_tp_point_builds_correct_cog_key():
    service = PointValueService()
    strategy = MagicMock()
    strategy.sample_cog_value = AsyncMock(return_value=0.0)
    service.set_strategy(strategy)

    await service.sample_ecmwf_tp_point(
        "20260330T1200Z", "20260330T1500Z", -34.0, -58.0
    )

    expected_key = (
        "cog/models/ecmwf/total_precipitation" "/20260330T1200Z/20260330T1500Z.tif"
    )
    strategy.sample_cog_value.assert_awaited_once_with(expected_key, -34.0, -58.0)


@pytest.mark.asyncio
async def test_sample_ecmwf_tp_point_raises_cog_not_found():
    service = PointValueService()
    strategy = MagicMock()
    strategy.sample_cog_value = AsyncMock(side_effect=CogObjectNotFoundError())
    service.set_strategy(strategy)

    with pytest.raises(CogNotFoundError):
        await service.sample_ecmwf_tp_point("20260330T1200Z", "p1", -34.0, -58.0)


@pytest.mark.asyncio
async def test_sample_ecmwf_tp_point_raises_nodata_or_outside():
    service = PointValueService()
    strategy = MagicMock()
    strategy.sample_cog_value = AsyncMock(side_effect=NoDataOrOutsideSampleError())
    service.set_strategy(strategy)

    with pytest.raises(NoDataOrOutsideError):
        await service.sample_ecmwf_tp_point("20260330T1200Z", "p1", -34.0, -58.0)


@pytest.mark.asyncio
async def test_sample_ecmwf_mslp_point_returns_hpa_unit():
    service = PointValueService()
    strategy = MagicMock()
    strategy.sample_cog_value = AsyncMock(return_value=1013.25)
    service.set_strategy(strategy)

    sample = await service.sample_ecmwf_mslp_point(
        "20260413T1200Z", "20260413T1500Z", -34.0, -58.0
    )

    assert sample.value == 1013.25
    assert sample.unit == "hPa"


@pytest.mark.asyncio
async def test_sample_ecmwf_mslp_point_builds_correct_cog_key():
    service = PointValueService()
    strategy = MagicMock()
    strategy.sample_cog_value = AsyncMock(return_value=1013.0)
    service.set_strategy(strategy)

    await service.sample_ecmwf_mslp_point(
        "20260413T1200Z", "20260413T1500Z", -34.0, -58.0
    )

    expected_key = (
        "cog/models/ecmwf/mean_sea_level_pressure" "/20260413T1200Z/20260413T1500Z.tif"
    )
    strategy.sample_cog_value.assert_awaited_once_with(expected_key, -34.0, -58.0)
