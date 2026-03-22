"""Satellite-specific models (GOES-19, ABI, etc.)."""

from typing import Dict, List
from pydantic import BaseModel
from models.base import ZoomLevels, BoundingBox, TilesetInfo


class ChannelConfig(BaseModel):
    """Configuration for a satellite channel (e.g., ch-13)."""

    name: str
    description: str
    zoom_levels: ZoomLevels
    bounding_box: BoundingBox
    tile_format: str = "webp"


class ChannelTilesetsResponse(BaseModel):
    """Response for listing tilesets of a specific channel."""

    product: str
    instrument: str
    channel: str
    channel_info: ChannelConfig
    tilesets: List[TilesetInfo]
    tile_url_pattern: str


class ChannelSummary(BaseModel):
    """Summary info for a channel in instrument listing."""

    name: str
    description: str
    available: bool = True


class InstrumentConfig(BaseModel):
    """Configuration for an instrument (e.g., ABI, GLM)."""

    name: str
    description: str
    channels: Dict[str, ChannelSummary]


class InstrumentResponse(BaseModel):
    """Response for a specific instrument."""

    product: str
    instrument: str
    instrument_info: InstrumentConfig
    endpoints: Dict[str, str]


class InstrumentSummary(BaseModel):
    """Summary info for an instrument in product listing."""

    name: str
    description: str
    available: bool = True


class SatelliteProductConfig(BaseModel):
    """Configuration for a satellite product (e.g., GOES-19)."""

    name: str
    description: str
    type: str  # satellite
    instruments: Dict[str, InstrumentSummary]


class SatelliteProductResponse(BaseModel):
    """Response for a specific satellite product."""

    product_id: str
    product_info: SatelliteProductConfig
    endpoints: Dict[str, str]


class SatellitePointValueResponse(BaseModel):
    """Response payload for point-value queries on satellite COGs."""

    product: str
    instrument: str
    channel: str
    tileset_id: str
    lat: float
    lon: float
    value: float
    unit: str
