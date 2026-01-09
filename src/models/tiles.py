"""Models for tiles endpoints."""
from typing import Dict, List, Optional
from pydantic import BaseModel


class ZoomLevels(BaseModel):
    """Zoom level configuration for a product."""
    min: int
    max: int


class ProductConfig(BaseModel):
    """Configuration for a tile product."""
    name: str
    description: str
    zoom_levels: ZoomLevels


class ProductsResponse(BaseModel):
    """Response model for listing products."""
    products: Dict[str, ProductConfig]
    tile_format: str
    tile_url_pattern: str


class TilesetInfo(BaseModel):
    """Information about a tileset."""
    id: str
    url_pattern: str


class TilesetsResponse(BaseModel):
    """Response model for listing tilesets."""
    product: str
    product_info: Optional[ProductConfig] = None
    tilesets: List[TilesetInfo]