from typing import Dict, List, Optional
from pydantic import BaseModel


# ============== Shared Models ==============

class ZoomLevels(BaseModel):
    """Zoom level configuration."""
    min: int
    max: int


class BoundingBox(BaseModel):
    """Geographic bounding box in EPSG:3857."""
    minx: float
    miny: float
    maxx: float
    maxy: float


# ============== Channel/Tileset Level Models ==============

class TilesetInfo(BaseModel):
    """Information about a tileset (timestamp-based)."""
    id: str
    url_pattern: str


class ChannelConfig(BaseModel):
    """Configuration for a channel (e.g., ch-13)."""
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


# ============== Instrument Level Models ==============

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


# ============== Product Level Models ==============

class InstrumentSummary(BaseModel):
    """Summary info for an instrument in product listing."""
    name: str
    description: str
    available: bool = True


class ProductConfig(BaseModel):
    """Configuration for a product (e.g., GOES-19)."""
    name: str
    description: str
    type: str  # satellite, radar, numerical_model, station
    instruments: Dict[str, InstrumentSummary]


class ProductResponse(BaseModel):
    """Response for a specific product."""
    product_id: str
    product_info: ProductConfig
    endpoints: Dict[str, str]


# ============== Top Level Models ==============

class ProductSummary(BaseModel):
    """Summary of a product for the products listing."""
    name: str
    description: str
    type: str
    available: bool = True


class ProductsListResponse(BaseModel):
    """Response for listing all products."""
    products: Dict[str, ProductSummary]