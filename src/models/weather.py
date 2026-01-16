"""Models for Weather.com API responses."""

from typing import Optional
from pydantic import BaseModel


class Geometry(BaseModel):
    """Geometry of the weather station."""

    coordinates: list[float]
    type: str


class Properties(BaseModel):
    """Properties of a weather station (EMA)."""

    id: Optional[str] = None
    country: str
    neighborhood: str
    adm1: str
    adm2: Optional[str] = None
    tempf: Optional[float] = None
    humidity: Optional[int] = None
    dewptf: Optional[float] = None
    heatindexf: Optional[float] = None
    windchillf: Optional[float] = None
    windspeedmph: Optional[float] = None
    windgustmph: Optional[float] = None
    winddir: Optional[int] = None
    baromin: Optional[float] = None
    rainin: Optional[float] = None
    dailyrainin: Optional[float] = None
    solarradiation: Optional[float] = None
    UV: Optional[float] = None
    dateutc: str
    validTime: int
    elev: Optional[int] = None
    qcStatus: int
    type: str
    platform: str


class WeatherFeature(BaseModel):
    """A single weather station feature."""

    type: str
    id: str
    geometry: Geometry
    properties: Properties


class WeatherTimeSlot(BaseModel):
    """Weather data for a specific time slot."""

    features: list[WeatherFeature]
