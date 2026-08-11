"""GFS model response schemas."""

from typing import List, Optional

from pydantic import BaseModel

from models.base import BoundingBox, ZoomLevels


class GfsCycleInfo(BaseModel):
    """Summary of a single GFS cycle (model run)."""

    cycle: str
    step_count: int


class GfsCycleListResponse(BaseModel):
    """Response listing the available cycles of a GFS product."""

    product_id: str
    cycles: List[GfsCycleInfo]
    layers: List[str]
    tile_url_pattern: Optional[str]
    zoom_levels: ZoomLevels
    bounding_box: BoundingBox


class GfsStepInfo(BaseModel):
    """One forecast step within a cycle.

    `valid_ts` is the timestamp the step is valid for (cycle + offset). The
    frontend animates in valid time, so serving it saves every consumer from
    re-deriving it out of `fxxx`.
    """

    fxxx: str
    valid_ts: str
    layers: List[str] = []


class GfsStepListResponse(BaseModel):
    """Response listing the forecast steps of one GFS cycle."""

    product_id: str
    cycle: str
    steps: List[GfsStepInfo]
    tile_url_pattern: Optional[str]
    zoom_levels: ZoomLevels
    bounding_box: BoundingBox


class GfsPointValueResponse(BaseModel):
    """Response for a point-value query against a GFS COG."""

    product_id: str
    cycle: str
    fxxx: str
    lat: float
    lon: float
    value: float
    unit: str
