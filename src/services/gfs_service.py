"""Service exposing GFS tiles, overlays and listings."""

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from dependencies import settings
from models.base import BoundingBox, ZoomLevels
from models.gfs import (
    GfsCycleInfo,
    GfsCycleListResponse,
    GfsStepInfo,
    GfsStepListResponse,
)
from services.gfs_config import (
    GFS_BARB_ZOOM_LEVELS,
    GFS_ZOOM_MAX,
    GFS_ZOOM_MIN,
    get_product,
    layers_for,
)
from services.gfs_sync_strategy import GfsSyncStrategy

_CYCLE_FORMAT = "%Y%m%dT%H%MZ"
_STEP_PATTERN = re.compile(r"^f(\d{3})$")


class GfsService:
    """Serves the three GFS products off a single read strategy."""

    ZOOM_LEVELS = ZoomLevels(min=GFS_ZOOM_MIN, max=GFS_ZOOM_MAX)
    BOUNDING_BOX = BoundingBox(minx=-110.0, miny=-60.0, maxx=-30.0, maxy=-15.0)
    TILE_URL_PATTERN = "/products/gfs/{product_id}/{cycle}/{fxxx}/{z}/{x}/{y}.webp"
    BARB_TILE_URL_PATTERN = (
        "/products/gfs/{product_id}/{cycle}/{fxxx}/barbs/{z}/{x}/{y}.json"
    )

    def __init__(self) -> None:
        self._strategy: Optional[GfsSyncStrategy] = None

    def set_strategy(self, strategy: GfsSyncStrategy) -> None:
        """Set the read strategy (called during app startup)."""
        self._strategy = strategy

    async def list_cycles(self, product_id: str) -> Optional[GfsCycleListResponse]:
        """Cycles available for a product, or None when the product is unknown."""
        product = get_product(product_id)
        if product is None:
            return None

        infos: List[GfsCycleInfo] = []
        if self._strategy is not None:
            for cycle in await self._advertised_cycles(product_id):
                steps = await self._strategy.list_steps(product_id, cycle)
                infos.append(GfsCycleInfo(cycle=cycle, step_count=len(steps)))

        return GfsCycleListResponse(
            product_id=product_id,
            cycles=infos,
            layers=layers_for(product_id),
            tile_url_pattern=self._tile_url_pattern(product_id),
            barb_tile_url_pattern=self._barb_url_pattern(product_id),
            barb_zoom_levels=self._barb_zoom_levels(product_id),
            zoom_levels=self.ZOOM_LEVELS,
            bounding_box=self.BOUNDING_BOX,
        )

    async def list_steps(
        self, product_id: str, cycle: str
    ) -> Optional[GfsStepListResponse]:
        """Steps of one cycle, or None when the product or cycle is unknown."""
        product = get_product(product_id)
        if product is None or self._strategy is None:
            return None

        if cycle not in await self._advertised_cycles(product_id):
            return None

        steps = await self._strategy.list_steps(product_id, cycle)
        if not steps:
            return None

        # Hydrate each step's overlay list from the index, concurrently. Reading
        # it per step (rather than reusing the product catalogue) is what keeps
        # the listing honest while a cycle is still filling in: an overlay that
        # tiles-processor has not uploaded yet is simply not advertised, so the
        # frontend never asks for a layer that would 404.
        layer_lists = await asyncio.gather(
            *(self._strategy.list_layers(product_id, cycle, s) for s in steps)
        )
        return GfsStepListResponse(
            product_id=product_id,
            cycle=cycle,
            steps=[
                GfsStepInfo(
                    fxxx=fxxx,
                    valid_ts=valid_timestamp(cycle, fxxx) or "",
                    layers=layers,
                )
                for fxxx, layers in zip(steps, layer_lists)
            ],
            tile_url_pattern=self._tile_url_pattern(product_id),
            barb_tile_url_pattern=self._barb_url_pattern(product_id),
            barb_zoom_levels=self._barb_zoom_levels(product_id),
            zoom_levels=self.ZOOM_LEVELS,
            bounding_box=self.BOUNDING_BOX,
        )

    async def _advertised_cycles(self, product_id: str) -> List[str]:
        """The cycles the API publishes: the newest `gfs_cycles_to_keep`.

        Capping here as well as in the read strategy keeps a stale index from
        leaking a retired cycle into the contract — the sync loop prunes that
        index, so a single failure there used to be enough to over-advertise.
        Same belt-and-braces ECMWF applies before listing forecasts.
        """
        if self._strategy is None:
            return []
        cycles = await self._strategy.list_cycles(product_id)
        return cycles[: settings.gfs_cycles_to_keep]

    async def get_tile_data(
        self, product_id: str, cycle: str, fxxx: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Raster tile bytes, or None when absent or the product has no raster."""
        product = get_product(product_id)
        if product is None or not product.has_tiles or self._strategy is None:
            return None
        return await self._strategy.get_tile(product_id, cycle, fxxx, z, x, y)

    async def get_geojson(
        self, product_id: str, cycle: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        """Overlay bytes, or None when the layer does not belong to the product."""
        product = get_product(product_id)
        if product is None or layer not in product.layers or self._strategy is None:
            return None
        return await self._strategy.get_geojson(product_id, cycle, fxxx, layer)

    async def get_barb_tile(
        self, product_id: str, cycle: str, fxxx: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Barb tile bytes, or None when the product carries no barbs."""
        product = get_product(product_id)
        if product is None or not product.has_barbs or self._strategy is None:
            return None
        return await self._strategy.get_barb_tile(product_id, cycle, fxxx, z, x, y)

    def _tile_url_pattern(self, product_id: str) -> Optional[str]:
        """Tile pattern, or None for a contour-only product like `mslp`."""
        product = get_product(product_id)
        if product is None or not product.has_tiles:
            return None
        return self.TILE_URL_PATTERN

    def _barb_url_pattern(self, product_id: str) -> Optional[str]:
        """Barb-tile pattern, or None for a product that carries no barbs."""
        product = get_product(product_id)
        if product is None or not product.has_barbs:
            return None
        return self.BARB_TILE_URL_PATTERN

    def _barb_zoom_levels(self, product_id: str) -> List[int]:
        """Native barb zooms, or empty for a product that carries no barbs."""
        product = get_product(product_id)
        if product is None or not product.has_barbs:
            return []
        return list(GFS_BARB_ZOOM_LEVELS)


def valid_timestamp(cycle: str, fxxx: str) -> Optional[str]:
    """Timestamp a step is valid for: cycle plus the forecast offset.

    Returns None when either part is malformed, so a stray S3 key cannot make
    the listing endpoint fail.
    """
    match = _STEP_PATTERN.match(fxxx)
    if match is None:
        return None
    try:
        base = datetime.strptime(cycle, _CYCLE_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return (base + timedelta(hours=int(match.group(1)))).strftime(_CYCLE_FORMAT)


gfs_service = GfsService()
