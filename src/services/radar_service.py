"""Service for radar products."""

from pathlib import Path
from typing import Optional

from services.base_service import BaseProductService
from clients.redis_client import RedisClient
from dependencies import logger


class RadarService(BaseProductService):
    """Service to manage radar products and tiles via Redis."""

    OUTPUT_RADAR_PATH = Path.cwd().parent / "output_radar"

    def __init__(self):
        self._redis_client: Optional[RedisClient] = None

    def set_redis_client(self, redis_client: RedisClient) -> None:
        """Set the Redis client (called during app startup)."""
        self._redis_client = redis_client

    async def list_radars(self):
        """List all available radars from Redis index."""
        if not self._redis_client:
            return {"radars": []}
        radars = await self._redis_client.get_radar_radars()
        return {"radars": radars}

    async def list_radar_variables(self, radar_id: str):
        """List all variables for a given radar from Redis index."""
        if not self._redis_client:
            return {"radar": radar_id, "variables": []}
        variables = await self._redis_client.get_radar_variables(radar_id)
        return {"radar": radar_id, "variables": variables}

    async def list_radar_elevations(self, radar_id: str, variable_id: str):
        """List all elevations for a given radar and variable from Redis index."""
        if not self._redis_client:
            return {
                "radar": radar_id,
                "variable": variable_id,
                "elevations": [],
            }
        elevations = await self._redis_client.get_radar_elevations(
            radar_id, variable_id
        )
        return {
            "radar": radar_id,
            "variable": variable_id,
            "elevations": elevations,
        }

    async def list_radar_tilesets(
        self, radar_id: str, variable_id: str, elevation_id: str
    ):
        """List all tilesets for a radar, variable, and elevation from Redis index."""
        if not self._redis_client:
            return {
                "radar": radar_id,
                "variable": variable_id,
                "elevation": elevation_id,
                "tilesets": [],
            }
        tilesets = await self._redis_client.get_radar_tilesets(
            radar_id, variable_id, elevation_id
        )
        return {
            "radar": radar_id,
            "variable": variable_id,
            "elevation": elevation_id,
            "tilesets": tilesets,
        }

    async def get_tile_data(
        self, radar_id, variable_id, elevation_id, tileset_id, z, x, y
    ) -> Optional[bytes]:
        """Get radar tile data from Redis."""
        if not self._redis_client:
            return None
        return await self._redis_client.get_radar_tile(
            radar_id, variable_id, tileset_id, elevation_id, z, x, y
        )


# Singleton instance
radar_service = RadarService()
