"""Service for WRF model tiles and overlay GeoJSONs."""

import asyncio
from typing import List, Optional

from models.base import BoundingBox, ZoomLevels
from models.wrf import (
    WrfInitRunInfo,
    WrfInitRunListResponse,
    WrfStepInfo,
    WrfStepListResponse,
)
from services.base_service import BaseProductService
from services.wrf_sync_strategy import WrfSyncStrategy


class WrfService(BaseProductService):
    """Service managing WRF model tile data + GeoJSON overlay layers."""

    ZOOM_LEVELS = ZoomLevels(min=4, max=6)
    BOUNDING_BOX = BoundingBox(minx=-110.0, miny=-60.0, maxx=-30.0, maxy=-15.0)
    TILE_URL_PATTERN = "/products/wrf/{product_id}/{init_tag}/{fxxx}/{z}/{x}/{y}.webp"

    def __init__(self) -> None:
        self._strategy: Optional[WrfSyncStrategy] = None

    def set_strategy(self, strategy: WrfSyncStrategy) -> None:
        """Set the sync strategy (called during app startup)."""
        self._strategy = strategy

    async def list_init_runs(self, product_id: str) -> WrfInitRunListResponse:
        """Return all init runs for a WRF product with step counts."""
        if not self._strategy:
            return WrfInitRunListResponse(
                product_id=product_id,
                init_runs=[],
                tile_url_pattern=self.TILE_URL_PATTERN,
                zoom_levels=self.ZOOM_LEVELS,
                bounding_box=self.BOUNDING_BOX,
            )

        init_runs = await self._strategy.list_init_runs(product_id)
        infos: List[WrfInitRunInfo] = []
        for init_tag in init_runs:
            steps = await self._strategy.list_steps(product_id, init_tag)
            infos.append(WrfInitRunInfo(init_tag=init_tag, step_count=len(steps)))

        return WrfInitRunListResponse(
            product_id=product_id,
            init_runs=infos,
            tile_url_pattern=self.TILE_URL_PATTERN,
            zoom_levels=self.ZOOM_LEVELS,
            bounding_box=self.BOUNDING_BOX,
        )

    async def list_steps(
        self, product_id: str, init_tag: str
    ) -> Optional[WrfStepListResponse]:
        """Return all forecast steps for a WRF product init run, or None if not found."""
        if not self._strategy:
            return None

        init_runs = await self._strategy.list_init_runs(product_id)
        if init_tag not in init_runs:
            return None

        steps = await self._strategy.list_steps(product_id, init_tag)
        # Hydrate per-step GeoJSON layers concurrently — listing is cheap and
        # callers (frontend) need the layer list to know which overlays to fetch.
        layer_lists = await asyncio.gather(
            *(self._strategy.list_layers(product_id, init_tag, s) for s in steps)
        )
        step_infos = [
            WrfStepInfo(fxxx=s, layers=layers)
            for s, layers in zip(steps, layer_lists)
        ]
        return WrfStepListResponse(
            product_id=product_id,
            init_tag=init_tag,
            steps=step_infos,
            tile_url_pattern=self.TILE_URL_PATTERN,
            zoom_levels=self.ZOOM_LEVELS,
            bounding_box=self.BOUNDING_BOX,
        )

    async def get_tile_data(
        self, product_id: str, init_tag: str, fxxx: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Get tile bytes via the configured strategy."""
        if not self._strategy:
            return None
        return await self._strategy.get_tile(product_id, init_tag, fxxx, z, x, y)

    async def get_geojson(
        self, product_id: str, init_tag: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        """Get GeoJSON layer bytes (barbs / contours) via the configured strategy."""
        if not self._strategy:
            return None
        return await self._strategy.get_geojson(product_id, init_tag, fxxx, layer)

    async def get_barb_tile(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Get rasterized wind-barb WebP tile bytes via the configured strategy."""
        if not self._strategy:
            return None
        return await self._strategy.get_barb_tile(
            product_id, init_tag, fxxx, z, x, y
        )


# Singleton instance
wrf_service = WrfService()
