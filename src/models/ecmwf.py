"""ECMWF total precipitation response models."""

from typing import List

from pydantic import BaseModel

from models.base import BoundingBox, ZoomLevels


class ForecastRunInfo(BaseModel):
    """Summary of a single ECMWF forecast run."""

    forecast_ts: str
    period_count: int


class ForecastListResponse(BaseModel):
    """Response listing available ECMWF forecast runs."""

    forecasts: List[ForecastRunInfo]


class PeriodInfo(BaseModel):
    """Info for a single centered 6h precipitation accumulation window."""

    period_ts: str


class PeriodListResponse(BaseModel):
    """Response listing all periods within a forecast run."""

    forecast_ts: str
    periods: List[PeriodInfo]
    tile_url_pattern: str
    zoom_levels: ZoomLevels
    bounding_box: BoundingBox


class EcmwfPointValueResponse(BaseModel):
    """Response for a point-value query on an ECMWF COG."""

    forecast_ts: str
    period_ts: str
    lat: float
    lon: float
    value: float
    unit: str
