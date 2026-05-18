"""ECMWF mean sea level pressure response models."""

from typing import List

from pydantic import BaseModel

from models.base import BoundingBox


class TimestampInfo(BaseModel):
    """Info for a single MSLP snapshot (instantaneous field at timestamp_ts, the period-end)."""

    timestamp_ts: str


class MslpForecastRunInfo(BaseModel):
    """Summary of a single ECMWF MSLP forecast run."""

    forecast_ts: str
    timestamp_count: int


class MslpForecastListResponse(BaseModel):
    """Response listing available ECMWF MSLP forecast runs."""

    forecasts: List[MslpForecastRunInfo]


class MslpTimestampListResponse(BaseModel):
    """Response listing all MSLP timestamps within a forecast run."""

    forecast_ts: str
    timestamps: List[TimestampInfo]
    bounding_box: BoundingBox


class EcmwfMslpPointValueResponse(BaseModel):
    """Response for a point-value query on an ECMWF MSLP COG."""

    forecast_ts: str
    timestamp_ts: str
    lat: float
    lon: float
    value: float
    unit: str
