"""Radar-specific models."""

from typing import Dict, List, Optional
from pydantic import BaseModel
from models.base import ZoomLevels, TilesetInfo


class ElevationInfo(BaseModel):
    """Information about a radar elevation angle."""

    id: str  # elev0, elev1, elev2
    angle: float  # 0.5, 0.9, 1.3 degrees
    description: str


class RadarStationInfo(BaseModel):
    """Information about a radar station."""

    id: str  # RMA3, RMA4, RMA9
    available: bool = True


class RadarVariableConfig(BaseModel):
    """Configuration for a radar variable (e.g., DBZH)."""

    name: str
    unit: str
    zoom_levels: ZoomLevels
    tile_format: str = "webp"


class RadarVariableSummary(BaseModel):
    """Summary info for a radar variable in product listing."""

    name: str
    unit: str
    available: bool = True


class RadarProductResponse(BaseModel):
    """Response for radar product showing available variables."""

    product_id: str
    product_info: Dict[str, str]
    variables: Dict[str, RadarVariableSummary]
    zoom_levels: ZoomLevels
    elevations: List[ElevationInfo]
    endpoints: Dict[str, str]


class RadarVariableResponse(BaseModel):
    """Response for a specific radar variable showing available stations."""

    product: str
    variable: str
    stations: Dict[str, RadarStationInfo]
    endpoints: Dict[str, str]


class RadarStationTilesetsResponse(BaseModel):
    """Response for listing tilesets of a specific radar station/variable."""

    product: str
    variable: str
    station: str
    station_info: RadarStationInfo
    variable_info: RadarVariableConfig
    elevations: List[ElevationInfo]
    tilesets: List[TilesetInfo]
    tile_url_pattern: str
