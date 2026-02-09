"""Service for radar products."""

from pathlib import Path
from typing import Optional

from services.base_service import BaseProductService
from services.radar_sync_strategy import RadarSyncStrategy


class RadarService(BaseProductService):
    """Service to manage radar products and tiles via sync strategy."""

    OUTPUT_RADAR_PATH = Path.cwd().parent / "output_radar"

    def __init__(self):
        self._strategy: Optional[RadarSyncStrategy] = None

    def set_strategy(self, strategy: RadarSyncStrategy) -> None:
        """Set the sync strategy (called during app startup)."""
        self._strategy = strategy

    # ============== Listing Methods ==============

    async def list_radars(self):
        """List all available radars."""
        if not self._strategy:
            return {"radars": []}

        radars = await self._strategy.list_radars()
        return {"radars": radars}

    async def list_radar_variables(self, radar_id: str):
        """List all variables for a given radar."""
        if not self._strategy:
            return {"radar": radar_id, "variables": []}

        variables = await self._strategy.list_variables(radar_id)
        return {"radar": radar_id, "variables": variables}

    async def list_radar_elevations(self, radar_id: str, variable_id: str):
        """List all elevations for a given radar and variable."""
        if not self._strategy:
            return {
                "radar": radar_id,
                "variable": variable_id,
                "elevations": [],
            }

        elevations = await self._strategy.list_elevations(radar_id, variable_id)
        return {
            "radar": radar_id,
            "variable": variable_id,
            "elevations": elevations,
        }

    async def list_radar_tilesets(
        self, radar_id: str, variable_id: str, elevation_id: str
    ):
        """List all tilesets for a radar, variable, and elevation."""
        if not self._strategy:
            return {
                "radar": radar_id,
                "variable": variable_id,
                "elevation": elevation_id,
                "tilesets": [],
            }

        tilesets = await self._strategy.list_tilesets(
            radar_id, variable_id, elevation_id
        )
        return {
            "radar": radar_id,
            "variable": variable_id,
            "elevation": elevation_id,
            "tilesets": tilesets,
        }

    # ============== Tile Serving ==============

    async def get_tile_data(
        self, radar_id, variable_id, elevation_id, tileset_id, z, x, y
    ) -> Optional[bytes]:
        """Get radar tile data via the sync strategy."""
        if not self._strategy:
            return None

        return await self._strategy.get_tile(
            radar_id, variable_id, elevation_id, tileset_id, z, x, y
        )


# Singleton instance
radar_service = RadarService()
