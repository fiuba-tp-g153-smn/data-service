"""Service for ECMWF total precipitation tiles and COGs."""

from typing import List, Optional

from dependencies import settings
from models.base import BoundingBox, ZoomLevels
from models.ecmwf_tp import (
    ForecastListResponse,
    ForecastRunInfo,
    PeriodInfo,
    PeriodListResponse,
)
from services.base_service import BaseProductService
from services.ecmwf_tp_sync_strategy import EcmwfTpSyncStrategy


class EcmwfTotalPrecipitationService(BaseProductService):
    """Service managing ECMWF precipitation forecast tiles and COGs."""

    ZOOM_LEVELS = ZoomLevels(min=3, max=7)
    BOUNDING_BOX = BoundingBox(minx=-110.0, miny=-60.0, maxx=-30.0, maxy=-15.0)
    TILE_URL_PATTERN = (
        "/products/ecmwf/total-precipitation/{forecast_ts}/{period_ts}/{z}/{x}/{y}.webp"
    )

    def __init__(self) -> None:
        self._strategy: Optional[EcmwfTpSyncStrategy] = None

    def set_strategy(self, strategy: EcmwfTpSyncStrategy) -> None:
        """Set the sync strategy (called during app startup)."""
        self._strategy = strategy

    async def list_forecasts(self) -> ForecastListResponse:
        """Return the N most recent forecast runs with their period counts."""
        if not self._strategy:
            return ForecastListResponse(forecasts=[])

        all_forecasts = await self._strategy.list_forecasts()
        active = all_forecasts[: settings.ecmwf_forecasts_to_keep]

        infos: List[ForecastRunInfo] = []
        for forecast_ts in active:
            periods = await self._strategy.list_periods(forecast_ts)
            infos.append(
                ForecastRunInfo(forecast_ts=forecast_ts, period_count=len(periods))
            )

        return ForecastListResponse(forecasts=infos)

    async def list_periods(self, forecast_ts: str) -> Optional[PeriodListResponse]:
        """Return all periods for a forecast run, or None if not found."""
        if not self._strategy:
            return None

        all_forecasts = await self._strategy.list_forecasts()
        if forecast_ts not in all_forecasts[: settings.ecmwf_forecasts_to_keep]:
            return None

        periods = await self._strategy.list_periods(forecast_ts)
        return PeriodListResponse(
            forecast_ts=forecast_ts,
            periods=[PeriodInfo(period_ts=p) for p in periods],
            tile_url_pattern=self.TILE_URL_PATTERN,
            zoom_levels=self.ZOOM_LEVELS,
            bounding_box=self.BOUNDING_BOX,
        )

    async def get_tile_data(
        self, forecast_ts: str, period_ts: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile bytes via the configured strategy."""
        if not self._strategy:
            return None
        return await self._strategy.get_tile(forecast_ts, period_ts, z, x, y)


ecmwf_tp_service = EcmwfTotalPrecipitationService()
