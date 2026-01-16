"""Service for radar products."""

from pathlib import Path
from typing import Dict, List, Optional, Tuple

from services.base_service import BaseProductService
from dependencies import logger


class RadarService(BaseProductService):
    """Service to manage radar products and tiles."""

    # Default zoom levels for Radar
    RADAR_ZOOM_LEVELS = {"min": 4, "max": 10}

    # Radar variables (productos meteorológicos)
    RADAR_VARIABLES: Dict[str, dict] = {
        "dbzh": {
            "name": "DBZH",
            "unit": "dBZ",
            "available": True,
            "dir_name": "DBZH",
        },
        "zdr": {
            "name": "ZDR",
            "unit": "dB",
            "available": False,
            "dir_name": "ZDR",
        },
        "rhohv": {
            "name": "RHOHV",
            "unit": "adimensional",
            "available": False,
            "dir_name": "RHOHV",
        },
        "kdp": {
            "name": "KDP",
            "unit": "°/km",
            "available": False,
            "dir_name": "KDP",
        },
    }

    # Radar stations (estaciones de radar)
    RADAR_STATIONS: Dict[str, dict] = {
        "rma3": {
            "id": "RMA3",
            "available": True,
        },
        "rma4": {
            "id": "RMA4",
            "available": True,
        },
        "rma9": {
            "id": "RMA9",
            "available": True,
        },
    }

    # Radar elevation angles
    RADAR_ELEVATIONS: List[dict] = [
        {"id": "elev0", "angle": 0.5, "description": "Elevación 0.5°"},
        {"id": "elev1", "angle": 0.9, "description": "Elevación 0.9°"},
        {"id": "elev2", "angle": 1.3, "description": "Elevación 1.3°"},
    ]

    def __init__(self):
        """Initialize and register radar product."""
        # Check if any variable is available
        has_available = any(
            var.get("available", False) for var in self.RADAR_VARIABLES.values()
        )

        # Register radar in the global registry
        self.register_product(
            "radar",
            {
                "name": "Red de Radares",
                "description": "Red de Radares Meteorológicos de Argentina (SMN)",
                "type": "radar",
                "available": has_available,
            },
        )

    # ============== Radar Product Methods ==============

    def get_radar_product(self) -> dict:
        """Get radar product with available variables."""
        variables_summary = {}
        for var_id, var_config in self.RADAR_VARIABLES.items():
            variables_summary[var_id] = {
                "name": var_config["name"],
                "unit": var_config["unit"],
                "available": var_config.get("available", False),
            }

        return {
            "product_id": "radar",
            "product_info": {
                "name": "Red de Radares",
                "description": "Red de Radares Meteorológicos de Argentina (SMN)",
                "type": "radar",
            },
            "variables": variables_summary,
            "zoom_levels": self.RADAR_ZOOM_LEVELS,
            "elevations": self.RADAR_ELEVATIONS,
            "endpoints": {
                var_id: f"/products/radar/{var_id}"
                for var_id, var_config in self.RADAR_VARIABLES.items()
                if var_config.get("available", False)
            },
        }

    def variable_exists(self, variable_id: str) -> bool:
        """Check if a radar variable exists."""
        return variable_id.lower() in self.RADAR_VARIABLES

    def get_variable(self, variable_id: str) -> Optional[dict]:
        """Get radar variable configuration with available stations."""
        variable = self.RADAR_VARIABLES.get(variable_id.lower())
        if not variable:
            return None

        # Build stations info
        stations_info = {}
        for station_id, station_config in self.RADAR_STATIONS.items():
            stations_info[station_id] = {
                "id": station_config["id"],
                "available": station_config.get("available", False),
            }

        return {
            "product": "radar",
            "variable": variable_id.lower(),
            "stations": stations_info,
            "endpoints": {
                station_id: f"/products/radar/{variable_id.lower()}/{station_id}"
                for station_id, station_config in self.RADAR_STATIONS.items()
                if station_config.get("available", False)
            },
        }

    def station_exists(self, station_id: str) -> bool:
        """Check if a radar station exists."""
        return station_id.lower() in self.RADAR_STATIONS

    def get_station_tilesets(self, variable_id: str, station_id: str) -> Optional[dict]:
        """Get available tilesets for a radar station and variable."""
        variable = self.RADAR_VARIABLES.get(variable_id.lower())
        station = self.RADAR_STATIONS.get(station_id.lower())

        if not variable or not station:
            return None

        # Get tilesets from filesystem
        tilesets = self._get_tilesets(variable_id, station_id)

        return {
            "product": "radar",
            "variable": variable_id.lower(),
            "station": station_id.lower(),
            "station_info": {
                "id": station["id"],
                "available": station.get("available", False),
            },
            "variable_info": {
                "name": variable["name"],
                "unit": variable["unit"],
                "zoom_levels": self.RADAR_ZOOM_LEVELS,
                "tile_format": "webp",
            },
            "elevations": self.RADAR_ELEVATIONS,
            "tilesets": tilesets,
            "tile_url_pattern": f"/products/radar/{variable_id.lower()}/{station_id.lower()}/{{elevation_id}}/{{tileset_id}}/{{z}}/{{x}}/{{y}}.webp",
        }

    def _get_tilesets(self, variable_id: str, station_id: str) -> List[dict]:
        """Get list of available tilesets for a radar station/variable."""
        variable = self.RADAR_VARIABLES.get(variable_id.lower())
        station = self.RADAR_STATIONS.get(station_id.lower())

        if not variable or not station:
            return []

        dir_name = variable["dir_name"]
        station_prefix = station["id"]

        radar_dir = self.TILES_BASE_PATH / "output_radar" / dir_name
        logger.info(f"Looking for radar tilesets in: {radar_dir}")

        if not radar_dir.exists():
            logger.info(f"Radar directory does not exist: {radar_dir}")
            return []

        tilesets = []
        seen_timestamps = set()

        # Look for directories matching the pattern: {STATION}_{VARIABLE}_{TIMESTAMP}_elev{N}
        for item in radar_dir.iterdir():
            if item.is_dir() and item.name.startswith(f"{station_prefix}_{dir_name}_"):
                parts = item.name.split("_")
                if len(parts) >= 4:
                    timestamp = parts[2]
                    if timestamp not in seen_timestamps:
                        seen_timestamps.add(timestamp)
                        tilesets.append(
                            {
                                "id": timestamp,
                                "url_pattern": f"/products/radar/{variable_id.lower()}/{station_id.lower()}/{{elevation_id}}/{timestamp}/{{z}}/{{x}}/{{y}}.webp",
                            }
                        )

        # Sort by timestamp (most recent first)
        tilesets.sort(key=lambda x: x["id"], reverse=True)

        logger.info(
            f"Found {len(tilesets)} radar tilesets for {station_prefix}/{dir_name}"
        )
        return tilesets

    def elevation_exists(self, elevation_id: str) -> bool:
        """Check if an elevation exists."""
        return any(elev["id"] == elevation_id for elev in self.RADAR_ELEVATIONS)

    def get_tile_path(
        self,
        variable_id: str,
        station_id: str,
        elevation_id: str,
        tileset_id: str,
        z: int,
        x: int,
        y: int,
    ) -> Path:
        """Build the full path to a radar tile file."""
        variable = self.RADAR_VARIABLES.get(variable_id.lower())
        station = self.RADAR_STATIONS.get(station_id.lower())

        if not variable or not station:
            return Path("")

        dir_name = variable["dir_name"]
        station_prefix = station["id"]

        folder_name = f"{station_prefix}_{dir_name}_{tileset_id}_{elevation_id}"
        return (
            self.TILES_BASE_PATH
            / "output_radar"
            / dir_name
            / folder_name
            / "tiles"
            / str(z)
            / str(x)
            / f"{y}.webp"
        )

    def validate_zoom_level(self, z: int) -> Tuple[bool, str]:
        """Validate if a zoom level is valid for radar."""
        if z < self.RADAR_ZOOM_LEVELS["min"] or z > self.RADAR_ZOOM_LEVELS["max"]:
            return (
                False,
                f"Zoom level {z} not available. Valid range: {self.RADAR_ZOOM_LEVELS['min']}-{self.RADAR_ZOOM_LEVELS['max']}",
            )
        return True, ""


# Singleton instance
radar_service = RadarService()
