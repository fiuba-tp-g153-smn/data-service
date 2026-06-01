"""WRF model response schemas."""

from typing import List

from pydantic import BaseModel

from models.base import BoundingBox, ZoomLevels


class WrfInitRunInfo(BaseModel):
    """Summary of a single WRF model initialization run."""

    init_tag: str
    step_count: int


class WrfInitRunListResponse(BaseModel):
    """Response listing available WRF initialization runs for a product."""

    product_id: str
    init_runs: List[WrfInitRunInfo]
    tile_url_pattern: str
    zoom_levels: ZoomLevels
    bounding_box: BoundingBox


class WrfStepInfo(BaseModel):
    """Info for a single WRF forecast step."""

    fxxx: str
    layers: List[str] = []


class WrfStepListResponse(BaseModel):
    """Response listing all forecast steps within an initialization run."""

    product_id: str
    init_tag: str
    steps: List[WrfStepInfo]
    tile_url_pattern: str
    zoom_levels: ZoomLevels
    bounding_box: BoundingBox


class WrfPointValueResponse(BaseModel):
    """Response for a point-value query on a WRF primary-field COG."""

    product_id: str
    init_tag: str
    fxxx: str
    lat: float
    lon: float
    value: float
    unit: str
