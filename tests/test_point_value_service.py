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
