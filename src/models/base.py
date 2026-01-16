"""Base models shared across different product types."""

from typing import Dict
from pydantic import BaseModel


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


class TilesetInfo(BaseModel):
    """Information about a tileset (timestamp-based)."""

    id: str
    url_pattern: str


class ProductSummary(BaseModel):
    """Summary of a product for the products listing."""

    name: str
    description: str
    type: str
    available: bool = True


class ProductsListResponse(BaseModel):
    """Response for listing all products."""

    products: Dict[str, ProductSummary]
