"""Service for radar products."""

from pathlib import Path

from services.base_service import BaseProductService
from dependencies import logger


class RadarService(BaseProductService):
    """Service to manage radar products and tiles."""

    OUTPUT_RADAR_PATH = Path.cwd().parent / "output_radar"

    def list_radars(self):
        """List all available radars (folders in output_radar)."""
        logger.info(f"Listing all radars in: {self.OUTPUT_RADAR_PATH}")
        radars = []
        if self.OUTPUT_RADAR_PATH.exists():
            for radar_dir in self.OUTPUT_RADAR_PATH.iterdir():
                if radar_dir.is_dir():
                    radars.append(radar_dir.name)
        else:
            logger.warning(f"Radar path does not exist: {self.OUTPUT_RADAR_PATH}")
        return {"radars": radars}

    def list_radar_variables(self, radar_id: str):
        """List all variables for a given radar (folders in output_radar/{radar_id})."""
        radar_path = self.OUTPUT_RADAR_PATH / radar_id
        logger.info(f"Listing variables for radar: {radar_id} in {radar_path}")
        variables = []
        if radar_path.exists():
            for var_dir in radar_path.iterdir():
                if var_dir.is_dir():
                    variables.append(var_dir.name)
        else:
            logger.warning(f"Radar directory does not exist: {radar_path}")
        return {"radar": radar_id, "variables": variables}

    def list_radar_elevations(self, radar_id: str, variable_id: str):
        """List all elevations for a given radar and variable (parse folders in output_radar/{radar_id}/{variable_id})."""
        var_path = self.OUTPUT_RADAR_PATH / radar_id / variable_id
        logger.info(
            f"Listing elevations for radar: {radar_id}, variable: {variable_id} in {var_path}"
        )
        elevations = set()
        if var_path.exists():
            for ts_dir in var_path.iterdir():
                if ts_dir.is_dir():
                    # Folder name: {TIMESTAMP}_elev{N}
                    parts = ts_dir.name.split("_elev")
                    if len(parts) == 2:
                        elevations.add(f"elev{parts[1]}")
        else:
            logger.warning(f"Variable directory does not exist: {var_path}")
        return {
            "radar": radar_id,
            "variable": variable_id,
            "elevations": sorted(elevations),
        }

    def list_radar_tilesets(self, radar_id: str, variable_id: str, elevation_id: str):
        """List all tilesets (timestamps) for a radar, variable, and elevation."""
        var_path = self.OUTPUT_RADAR_PATH / radar_id / variable_id
        logger.info(
            f"Listing tilesets for radar: {radar_id}, variable: {variable_id}, elevation: {elevation_id} in {var_path}"
        )
        tilesets = []
        if var_path.exists():
            for ts_dir in var_path.iterdir():
                if ts_dir.is_dir() and ts_dir.name.endswith(f"_{elevation_id}"):
                    # Folder name: {TIMESTAMP}_elev{N}
                    ts = ts_dir.name.split("_elev")[0]
                    tilesets.append(ts)
        else:
            logger.warning(f"Variable directory does not exist: {var_path}")
        return {
            "radar": radar_id,
            "variable": variable_id,
            "elevation": elevation_id,
            "tilesets": sorted(tilesets, reverse=True),
        }

    def get_tile_path_output_radar(
        self, radar_id, variable_id, elevation_id, tileset_id, z, x, y
    ):
        """Build the path to a tile in output_radar."""
        folder_name = f"{tileset_id}_{elevation_id}"
        tile_path = (
            self.OUTPUT_RADAR_PATH
            / radar_id
            / variable_id
            / folder_name
            / "tiles"
            / str(z)
            / str(x)
            / f"{y}.webp"
        )
        logger.debug(f"Radar tile path resolved: {tile_path}")
        return tile_path


# Singleton instance
radar_service = RadarService()
