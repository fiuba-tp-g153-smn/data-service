"""Service for ECMWF mean sea level pressure isobars GeoJSON and COGs."""

from typing import List, Optional

from dependencies import settings
from models.base import BoundingBox
from models.ecmwf_mslp import (
    MslpForecastListResponse,
    MslpForecastRunInfo,
    MslpTimestampListResponse,
    TimestampInfo,
)
from services.base_service import BaseProductService
from services.ecmwf_mslp_sync_strategy import EcmwfMslpSyncStrategy


class EcmwfMslpService(BaseProductService):
    """Service managing ECMWF MSLP forecast COGs and isobars GeoJSONs."""

    BOUNDING_BOX = BoundingBox(minx=-110.0, miny=-60.0, maxx=-30.0, maxy=-15.0)

    def __init__(self) -> None:
        self._strategy: Optional[EcmwfMslpSyncStrategy] = None

    def set_strategy(self, strategy: EcmwfMslpSyncStrategy) -> None:
        """Set the sync strategy (called during app startup)."""
        self._strategy = strategy

    async def list_forecasts(self) -> MslpForecastListResponse:
        """Return the N most recent MSLP forecast runs with their timestamp counts."""
        if not self._strategy:
            return MslpForecastListResponse(forecasts=[])

        all_forecasts = await self._strategy.list_forecasts()
        active = all_forecasts[: settings.ecmwf_forecasts_to_keep]

        infos: List[MslpForecastRunInfo] = []
        for forecast_ts in active:
            timestamps = await self._strategy.list_timestamps(forecast_ts)
            infos.append(
                MslpForecastRunInfo(
                    forecast_ts=forecast_ts, timestamp_count=len(timestamps)
                )
            )

        return MslpForecastListResponse(forecasts=infos)

    async def list_timestamps(
        self, forecast_ts: str
    ) -> Optional[MslpTimestampListResponse]:
        """Return all timestamps for a forecast run, or None if not found."""
        if not self._strategy:
            return None

        all_forecasts = await self._strategy.list_forecasts()
        if forecast_ts not in all_forecasts[: settings.ecmwf_forecasts_to_keep]:
            return None

        timestamps = await self._strategy.list_timestamps(forecast_ts)
        return MslpTimestampListResponse(
            forecast_ts=forecast_ts,
            timestamps=[TimestampInfo(timestamp_ts=t) for t in timestamps],
            bounding_box=self.BOUNDING_BOX,
        )

    async def get_geojson(self, forecast_ts: str, timestamp_ts: str) -> Optional[bytes]:
        """Get isobars GeoJSON bytes via the configured strategy."""
        if not self._strategy:
            return None
        return await self._strategy.get_geojson(forecast_ts, timestamp_ts)


ecmwf_mslp_service = EcmwfMslpService()
