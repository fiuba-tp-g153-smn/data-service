"""Service for managing satellite and weather products tiles."""
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dependencies import logger


class ProductsService:
    """Service to manage and serve satellite/weather tiles products."""
    
    TILES_BASE_PATH = Path.cwd() / ".tmp"
    
    # Bounding box for Argentina/South America region (from GOES-19 ABI)
    # This is constant for all ABI channels as they cover the same region
    GOES19_ABI_BOUNDING_BOX = {
        "minx": -75.00696140269579,
        "miny": -56.00753990703775,
        "maxx": -52.97746988314092,
        "maxy": -20.98391841407633
    }
    
    # Default zoom levels for GOES-19 ABI
    GOES19_ABI_ZOOM_LEVELS = {"min": 3, "max": 7}
    
    # Default zoom levels for Radar
    RADAR_ZOOM_LEVELS = {"min": 4, "max": 10}
    
    # ============== Radar Configuration ==============
    
    # Radar variables (productos meteorológicos)
    RADAR_VARIABLES: Dict[str, dict] = {
        "dbzh": {
            "name": "DBZH",
            "description": "Reflectividad Horizontal (dBZ)",
            "unit": "dBZ",
            "available": True,
            "dir_name": "DBZH",  # Directory name in filesystem
        },
        "zdr": {
            "name": "ZDR",
            "description": "Reflectividad Diferencial",
            "unit": "dB",
            "available": False,  # Coming soon
            "dir_name": "ZDR",
        },
        "rhohv": {
            "name": "RHOHV",
            "description": "Coeficiente de Correlación",
            "unit": "adimensional",
            "available": False,  # Coming soon
            "dir_name": "RHOHV",
        },
        "kdp": {
            "name": "KDP",
            "description": "Fase Diferencial Específica",
            "unit": "°/km",
            "available": False,  # Coming soon
            "dir_name": "KDP",
        },
    }
    
    # Radar stations (estaciones de radar)
    RADAR_STATIONS: Dict[str, dict] = {
        "rma3": {
            "id": "RMA3",
            "name": "RMA3",
            "available": True,
        },
        "rma4": {
            "id": "RMA4",
            "name": "RMA4",
            "available": True,
        },
        "rma9": {
            "id": "RMA9",
            "name": "RMA9",
            "available": True,
        },
    }
    
    # Radar elevation angles
    RADAR_ELEVATIONS: List[dict] = [
        {"id": "elev0", "angle": 0.5, "description": "Elevación 0.5°"},
        {"id": "elev1", "angle": 0.9, "description": "Elevación 0.9°"},
        {"id": "elev2", "angle": 1.3, "description": "Elevación 1.3°"},
    ]
    
    # ============== Products Configuration ==============
    # Hierarchical structure: Product -> Instrument -> Channel
    
    PRODUCTS: Dict[str, dict] = {
        "goes-19": {
            "name": "GOES-19",
            "description": "Geostationary Operational Environmental Satellite 19",
            "type": "satellite",
            "instruments": {
                "abi": {
                    "name": "ABI",
                    "description": "Advanced Baseline Imager",
                    "available": True,
                    "channels": {
                        "ch-2": {
                            "name": "Channel 2",
                            "description": "Red Visible (0.64 µm)",
                            "available": False,  # Coming soon
                        },
                        "ch-9": {
                            "name": "Channel 9",
                            "description": "Mid-Level Water Vapor (6.9 µm)",
                            "available": False,  # Coming soon
                        },
                        "ch-13": {
                            "name": "Channel 13",
                            "description": "Clean IR Longwave Window (10.3 µm) - Cloud Top",
                            "available": True,
                        },
                    }
                },
                "glm": {
                    "name": "GLM",
                    "description": "Geostationary Lightning Mapper",
                    "available": False,  # Coming soon
                    "channels": {}
                }
            }
        },
        # Future products placeholders
        "radar": {
            "name": "Red de Radares",
            "description": "Red de Radares Meteorológicos de Argentina (SMN)",
            "type": "radar",
            "instruments": {}  # Radar uses a different structure (variables/stations)
        },
        "numerical-models": {
            "name": "Numerical Models",
            "description": "Numerical Weather Prediction Models",
            "type": "numerical_model",
            "instruments": {}
        },
        "emas": {
            "name": "EMAS",
            "description": "Automatic Weather Stations",
            "type": "station",
            "instruments": {}
        },
    }
    
    # Mapping from channel IDs to directory names
    CHANNEL_DIR_MAPPING = {
        "ch-2": "band_2",
        "ch-9": "band_9",
        "ch-13": "band_13",
    }
    
    # ============== Product Level Methods ==============
    
    def get_products_list(self) -> dict:
        """Get summary list of all available products."""
        products_summary = {}
        for product_id, config in self.PRODUCTS.items():
            # Check if product has any available instruments
            has_available = any(
                inst.get("available", False) 
                for inst in config.get("instruments", {}).values()
            )
            products_summary[product_id] = {
                "name": config["name"],
                "description": config["description"],
                "type": config["type"],
                "available": has_available
            }
        return {"products": products_summary, "api_version": "1.0"}
    
    def product_exists(self, product_id: str) -> bool:
        """Check if a product exists."""
        return product_id in self.PRODUCTS
    
    def get_product(self, product_id: str) -> Optional[dict]:
        """Get product configuration."""
        product = self.PRODUCTS.get(product_id)
        if not product:
            return None
        
        # Build instruments summary
        instruments_summary = {}
        for inst_id, inst_config in product.get("instruments", {}).items():
            instruments_summary[inst_id] = {
                "name": inst_config["name"],
                "description": inst_config["description"],
                "available": inst_config.get("available", False)
            }
        
        return {
            "product_id": product_id,
            "product_info": {
                "name": product["name"],
                "description": product["description"],
                "type": product["type"],
                "instruments": instruments_summary
            },
            "endpoints": {
                inst_id: f"/products/{product_id}/{inst_id}"
                for inst_id in product.get("instruments", {}).keys()
            }
        }
    
    # ============== Instrument Level Methods ==============
    
    def instrument_exists(self, product_id: str, instrument_id: str) -> bool:
        """Check if an instrument exists for a product."""
        product = self.PRODUCTS.get(product_id)
        if not product:
            return False
        return instrument_id in product.get("instruments", {})
    
    def get_instrument(self, product_id: str, instrument_id: str) -> Optional[dict]:
        """Get instrument configuration."""
        product = self.PRODUCTS.get(product_id)
        if not product:
            return None
        
        instrument = product.get("instruments", {}).get(instrument_id)
        if not instrument:
            return None
        
        # Build channels summary
        channels_summary = {}
        for ch_id, ch_config in instrument.get("channels", {}).items():
            channels_summary[ch_id] = {
                "name": ch_config["name"],
                "description": ch_config["description"],
                "available": ch_config.get("available", False)
            }
        
        return {
            "product": product_id,
            "instrument": instrument_id,
            "instrument_info": {
                "name": instrument["name"],
                "description": instrument["description"],
                "channels": channels_summary
            },
            "endpoints": {
                ch_id: f"/products/{product_id}/{instrument_id}/{ch_id}"
                for ch_id, ch_config in instrument.get("channels", {}).items()
                if ch_config.get("available", False)
            }
        }
    
    # ============== Channel Level Methods ==============
    
    def channel_exists(self, product_id: str, instrument_id: str, channel_id: str) -> bool:
        """Check if a channel exists for an instrument."""
        product = self.PRODUCTS.get(product_id)
        if not product:
            return False
        
        instrument = product.get("instruments", {}).get(instrument_id)
        if not instrument:
            return False
        
        return channel_id in instrument.get("channels", {})
    
    def get_channel_config(self, product_id: str, instrument_id: str, channel_id: str) -> Optional[dict]:
        """Get full channel configuration including zoom levels and bounding box."""
        product = self.PRODUCTS.get(product_id)
        if not product:
            return None
        
        instrument = product.get("instruments", {}).get(instrument_id)
        if not instrument:
            return None
        
        channel = instrument.get("channels", {}).get(channel_id)
        if not channel:
            return None
        
        # For GOES-19 ABI, use the predefined bounding box and zoom levels
        if product_id == "goes-19" and instrument_id == "abi":
            return {
                "name": channel["name"],
                "description": channel["description"],
                "zoom_levels": self.GOES19_ABI_ZOOM_LEVELS,
                "bounding_box": self.GOES19_ABI_BOUNDING_BOX,
                "tile_format": "webp"
            }
        
        # Default configuration for other products
        return {
            "name": channel["name"],
            "description": channel["description"],
            "zoom_levels": {"min": 1, "max": 10},
            "bounding_box": {"minx": -180, "miny": -90, "maxx": 180, "maxy": 90},
            "tile_format": "webp"
        }
    
    def get_channel_tilesets(self, product_id: str, instrument_id: str, channel_id: str) -> dict:
        """Get available tilesets for a channel with full metadata."""
        channel_config = self.get_channel_config(product_id, instrument_id, channel_id)
        tilesets = self._get_tilesets_for_channel(product_id, instrument_id, channel_id)
        
        tile_url_pattern = f"/products/{product_id}/{instrument_id}/{channel_id}/{{tileset_id}}/{{z}}/{{x}}/{{y}}.webp"
        
        return {
            "product": product_id,
            "instrument": instrument_id,
            "channel": channel_id,
            "channel_info": channel_config,
            "tilesets": tilesets,
            "tile_url_pattern": tile_url_pattern
        }
    
    def _get_tilesets_for_channel(self, product_id: str, instrument_id: str, channel_id: str) -> List[dict]:
        """Get list of available tilesets for a channel."""
        # Map channel ID to directory name
        dir_name = self.CHANNEL_DIR_MAPPING.get(channel_id, channel_id)
        tiles_dir = self.TILES_BASE_PATH / dir_name / "tiles"
        
        logger.info(f"Looking for tilesets in: {tiles_dir}")
        
        if not tiles_dir.exists():
            logger.info(f"Tiles directory does not exist: {tiles_dir}")
            return []
        
        tilesets = []
        for item in tiles_dir.iterdir():
            if item.is_dir() and item.name.endswith("_tiles"):
                tileset_id = item.name.replace("_tiles", "")
                tilesets.append({
                    "id": tileset_id,
                    "url_pattern": f"/products/{product_id}/{instrument_id}/{channel_id}/{tileset_id}/{{z}}/{{x}}/{{y}}.webp"
                })
        
        logger.info(f"Found {len(tilesets)} tilesets for {product_id}/{instrument_id}/{channel_id}")
        return tilesets
    
    # ============== Tile Serving Methods ==============
    
    def get_tile_path(self, product_id: str, instrument_id: str, channel_id: str, 
                      tileset_id: str, z: int, x: int, y: int) -> Path:
        """Build the full path to a tile file."""
        dir_name = self.CHANNEL_DIR_MAPPING.get(channel_id, channel_id)
        return self.TILES_BASE_PATH / dir_name / "tiles" / f"{tileset_id}_tiles" / str(z) / str(x) / f"{y}.webp"
    
    def validate_zoom_level(self, product_id: str, instrument_id: str, channel_id: str, z: int) -> Tuple[bool, str]:
        """Validate if a zoom level is valid for a channel."""
        config = self.get_channel_config(product_id, instrument_id, channel_id)
        if not config:
            return False, f"Channel '{channel_id}' not found"
        
        zoom_levels = config["zoom_levels"]
        if z < zoom_levels["min"] or z > zoom_levels["max"]:
            return False, f"Zoom level {z} not available. Valid range: {zoom_levels['min']}-{zoom_levels['max']}"
        
        return True, ""
    
    # ============== Radar Methods ==============
    
    def get_radar_product(self) -> dict:
        """Get radar product with available variables."""
        variables_summary = {}
        for var_id, var_config in self.RADAR_VARIABLES.items():
            variables_summary[var_id] = {
                "name": var_config["name"],
                "description": var_config["description"],
                "unit": var_config["unit"],
                "available": var_config.get("available", False)
            }
        
        return {
            "product_id": "radar",
            "product_info": {
                "name": "Red de Radares",
                "description": "Red de Radares Meteorológicos de Argentina (SMN)",
                "type": "radar"
            },
            "variables": variables_summary,
            "endpoints": {
                var_id: f"/products/radar/{var_id}"
                for var_id, var_config in self.RADAR_VARIABLES.items()
                if var_config.get("available", False)
            }
        }
    
    def radar_variable_exists(self, variable_id: str) -> bool:
        """Check if a radar variable exists."""
        return variable_id.lower() in self.RADAR_VARIABLES
    
    def get_radar_variable(self, variable_id: str) -> Optional[dict]:
        """Get radar variable configuration with available stations."""
        variable = self.RADAR_VARIABLES.get(variable_id.lower())
        if not variable:
            return None
        
        # Build stations info
        stations_info = {}
        for station_id, station_config in self.RADAR_STATIONS.items():
            stations_info[station_id] = {
                "id": station_config["id"],
                "name": station_config["name"],
                "description": station_config["description"],
                "available": station_config.get("available", False)
            }
        
        return {
            "product": "radar",
            "variable": variable_id.lower(),
            "variable_info": {
                "name": variable["name"],
                "description": variable["description"],
                "unit": variable["unit"],
                "zoom_levels": self.RADAR_ZOOM_LEVELS,
                "tile_format": "webp"
            },
            "stations": stations_info,
            "elevations": self.RADAR_ELEVATIONS,
            "endpoints": {
                station_id: f"/products/radar/{variable_id.lower()}/{station_id}"
                for station_id, station_config in self.RADAR_STATIONS.items()
                if station_config.get("available", False)
            }
        }
    
    def radar_station_exists(self, station_id: str) -> bool:
        """Check if a radar station exists."""
        return station_id.lower() in self.RADAR_STATIONS
    
    def get_radar_station_tilesets(self, variable_id: str, station_id: str) -> Optional[dict]:
        """Get available tilesets for a radar station and variable."""
        variable = self.RADAR_VARIABLES.get(variable_id.lower())
        station = self.RADAR_STATIONS.get(station_id.lower())
        
        if not variable or not station:
            return None
        
        # Get tilesets from filesystem
        tilesets = self._get_radar_tilesets(variable_id, station_id)
        
        return {
            "product": "radar",
            "variable": variable_id.lower(),
            "station": station_id.lower(),
            "station_info": {
                "id": station["id"],
                "name": station["name"],
                "available": station.get("available", False)
            },
            "variable_info": {
                "name": variable["name"],
                "description": variable["description"],
                "unit": variable["unit"],
                "zoom_levels": self.RADAR_ZOOM_LEVELS,
                "tile_format": "webp"
            },
            "elevations": self.RADAR_ELEVATIONS,
            "tilesets": tilesets,
            "tile_url_pattern": f"/products/radar/{variable_id.lower()}/{station_id.lower()}/{{elevation_id}}/{{tileset_id}}/{{z}}/{{x}}/{{y}}.webp"
        }
    
    def _get_radar_tilesets(self, variable_id: str, station_id: str) -> List[dict]:
        """Get list of available tilesets for a radar station/variable."""
        variable = self.RADAR_VARIABLES.get(variable_id.lower())
        station = self.RADAR_STATIONS.get(station_id.lower())
        
        if not variable or not station:
            return []
        
        dir_name = variable["dir_name"]
        station_prefix = station["id"]  # e.g., "RMA3"
        
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
                # Extract timestamp from directory name
                # Format: RMA3_DBZH_20251230T151917Z_elev0
                parts = item.name.split("_")
                if len(parts) >= 4:
                    timestamp = parts[2]  # e.g., "20251230T151917Z"
                    if timestamp not in seen_timestamps:
                        seen_timestamps.add(timestamp)
                        tilesets.append({
                            "id": timestamp,
                            "url_pattern": f"/products/radar/{variable_id.lower()}/{station_id.lower()}/{{elevation_id}}/{timestamp}/{{z}}/{{x}}/{{y}}.webp"
                        })
        
        # Sort by timestamp (most recent first)
        tilesets.sort(key=lambda x: x["id"], reverse=True)
        
        logger.info(f"Found {len(tilesets)} radar tilesets for {station_prefix}/{dir_name}")
        return tilesets
    
    def elevation_exists(self, elevation_id: str) -> bool:
        """Check if an elevation exists."""
        return any(elev["id"] == elevation_id for elev in self.RADAR_ELEVATIONS)
    
    def get_radar_tile_path(self, variable_id: str, station_id: str, elevation_id: str,
                           tileset_id: str, z: int, x: int, y: int) -> Path:
        """Build the full path to a radar tile file."""
        variable = self.RADAR_VARIABLES.get(variable_id.lower())
        station = self.RADAR_STATIONS.get(station_id.lower())
        
        if not variable or not station:
            return Path("")
        
        dir_name = variable["dir_name"]
        station_prefix = station["id"]
        
        # Path: .tmp/output_radar/DBZH/RMA3_DBZH_20251230T151917Z_elev0/tiles/z/x/y.webp
        folder_name = f"{station_prefix}_{dir_name}_{tileset_id}_{elevation_id}"
        return self.TILES_BASE_PATH / "output_radar" / dir_name / folder_name / "tiles" / str(z) / str(x) / f"{y}.webp"
    
    def validate_radar_zoom_level(self, z: int) -> Tuple[bool, str]:
        """Validate if a zoom level is valid for radar."""
        if z < self.RADAR_ZOOM_LEVELS["min"] or z > self.RADAR_ZOOM_LEVELS["max"]:
            return False, f"Zoom level {z} not available. Valid range: {self.RADAR_ZOOM_LEVELS['min']}-{self.RADAR_ZOOM_LEVELS['max']}"
        return True, ""


# Singleton instance
products_service = ProductsService()