"""GFS model response schemas."""

from typing import List, Optional

from pydantic import BaseModel

from models.base import BoundingBox, ZoomLevels


class GfsCycleInfo(BaseModel):
    """Summary of a single GFS cycle (model run)."""

    cycle: str
    step_count: int


class GfsCycleListResponse(BaseModel):
    """Response listing the available cycles of a GFS product.

    `layers` holds only the single-file overlays, i.e. exactly the names that
    resolve as `.../{fxxx}/{layer}.json`. Wind barbs are per-tile and are
    reported through `barb_tile_url_pattern` / `barb_zoom_levels` instead.
    """

    product_id: str
    cycles: List[GfsCycleInfo]
    layers: List[str]
    tile_url_pattern: Optional[str]
    barb_tile_url_pattern: Optional[str] = None
    barb_zoom_levels: List[int] = []
    zoom_levels: ZoomLevels
    bounding_box: BoundingBox


class GfsStepInfo(BaseModel):
    """One forecast step within a cycle.

    `valid_ts` is the timestamp the step is valid for (cycle + offset). The
    frontend animates in valid time, so serving it saves every consumer from
    re-deriving it out of `fxxx`.

    `layers` is what this step *actually* has, read from the overlay index — not
    what the product could carry. A cycle fills in gradually, so a step can be
    listed before all of its overlays exist.
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
    barb_tile_url_pattern: Optional[str] = None
    barb_zoom_levels: List[int] = []
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
