"""Catalogue of the GFS products published by `tiles-processor`.

One place describing, per product: how it is named in the API, where its objects
live in S3, which overlay layers it carries and what unit a point query returns.
Everything downstream (key builders, sync, routes) reads it from here so the
three descriptions cannot drift apart.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class GfsProduct:
    """One GFS product as exposed by the API."""

    product_id: str  # segment used in the URL
    s3_segment: str  # segment used in the S3 key
    layers: Tuple[str, ...]  # single-file GeoJSON overlays
    unit: str  # unit a point query returns
    has_tiles: bool  # whether a raster pyramid exists
    has_barbs: bool = False  # whether per-tile wind barbs exist


# `mslp` is abbreviated in the URL but keeps the long S3 segment written by
# tiles-processor. The other two match on both sides.
GFS_MSLP = GfsProduct(
    product_id="mslp",
    s3_segment="mean_sea_level_pressure",
    layers=("isobars", "thickness"),
    unit="hPa",
    # The SMN's `slpb.gs` chart is pure contours, so no raster is produced.
    has_tiles=False,
)

GFS_500HPA = GfsProduct(
    product_id="500hpa",
    s3_segment="500hpa",
    layers=("heights", "isotherms"),
    unit="kt",
    has_tiles=True,
    has_barbs=True,
)

GFS_250HPA = GfsProduct(
    product_id="250hpa",
    s3_segment="250hpa",
    layers=("heights",),
    unit="kt",
    has_tiles=True,
)

GFS_PRODUCTS: Dict[str, GfsProduct] = {
    product.product_id: product for product in (GFS_MSLP, GFS_500HPA, GFS_250HPA)
}

# Raster pyramids are cut for these zooms in tiles-processor.
GFS_ZOOM_MIN = 3
GFS_ZOOM_MAX = 7

# Wind barbs are only emitted at these zooms; the frontend overzooms above the
# deepest one. Mirrors `BARB_ZOOM_STRIDES` in tiles-processor.
GFS_BARB_ZOOM_LEVELS: Tuple[int, ...] = (2, 4, 6, 8)


def get_product(product_id: str) -> Optional[GfsProduct]:
    """Look up a product by its API id, or None when unknown."""
    return GFS_PRODUCTS.get(product_id)


def product_ids() -> List[str]:
    """All API product ids, in catalogue order."""
    return list(GFS_PRODUCTS)


def layers_for(product_id: str) -> List[str]:
    """Single-file overlay layers a product can carry.

    Barbs are deliberately excluded: unlike WRF, GFS has no `{step}_barbs.json`
    document — barbs exist only as per-tile objects under `{step}_barbs/z/x/y`
    and have their own route. Listing them here would advertise a name that
    404s when fetched as `{layer}.json`. They are reported instead through
    `barb_tile_url_pattern` / `barb_zoom_levels`.
    """
    product = GFS_PRODUCTS.get(product_id)
    if product is None:
        return []
    return list(product.layers)


# ============== S3 object-name parsing ==============
#
# tiles-processor names every object of a step `{cycle}_{fxxx}` while nesting it
# under the cycle. Both the sync loop and the read strategy have to undo that,
# so the rule lives here once: a rename upstream is a one-line fix, not a hunt.


def leaf_segment(prefix: str) -> str:
    """Last path segment of an S3 common prefix."""
    return prefix.rstrip("/").split("/")[-1]


def step_from_basename(basename: str, cycle: str) -> Optional[str]:
    """Turn `{cycle}_{fxxx}` back into `{fxxx}`, or None if it does not match."""
    marker = f"{cycle}_"
    if not basename.startswith(marker):
        return None
    step = basename[len(marker) :]
    return step or None


def step_and_layer_from_basename(
    basename: str, cycle: str
) -> Optional[Tuple[str, str]]:
    """Split an overlay basename `{cycle}_{fxxx}_{layer}` into `(fxxx, layer)`.

    Neither a step tag (`f003`) nor a layer name carries an underscore, so the
    first separator after the cycle is the boundary.
    """
    tail = step_from_basename(basename, cycle)
    if tail is None or "_" not in tail:
        return None
    fxxx, layer = tail.split("_", 1)
    if not fxxx or not layer:
        return None
    return fxxx, layer
