"""Service for satellite products (GOES-19, ABI, etc.)."""

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from dependencies import logger
from services.base_service import BaseProductService
from services.satellite_sync_strategy import SatelliteSyncStrategy


class SatelliteService(BaseProductService):
    """Service to manage satellite products and tiles."""

    # Bounding box for Argentina/South America region (from GOES-19 ABI)
    GOES19_ABI_BOUNDING_BOX = {
        "minx": -75.00696140269579,
        "miny": -56.00753990703775,
        "maxx": -52.97746988314092,
        "maxy": -20.98391841407633,
    }

    # Default zoom levels for GOES-19 ABI
    GOES19_ABI_ZOOM_LEVELS = {"min": 3, "max": 7}

    # Satellite products configuration
    SATELLITE_PRODUCTS: Dict[str, dict] = {
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
                            "available": False,
                        },
                        "ch-9": {
                            "name": "Channel 9",
                            "description": "Mid-Level Water Vapor (6.9 µm)",
                            "available": False,
                        },
                        "ch-13": {
                            "name": "Channel 13",
                            "description": "Clean IR Longwave Window (10.3 µm) - Cloud Top",
                            "available": True,
                        },
                    },
                },
                "glm": {
                    "name": "GLM",
                    "description": "Geostationary Lightning Mapper",
                    "available": False,
                    "channels": {},
                },
            },
        }
    }

    # Mapping from channel IDs to directory names
    CHANNEL_DIR_MAPPING = {
        "ch-2": "band_2",
        "ch-9": "band_9",
        "ch-13": "band_13",
    }

    def __init__(self):
        """Initialize and register satellite products."""
        self._strategy: Optional[SatelliteSyncStrategy] = None
        for product_id, config in self.SATELLITE_PRODUCTS.items():
            self.register_product(product_id, config)

    def set_strategy(self, strategy: SatelliteSyncStrategy) -> None:
        """Set the sync strategy (called during app startup)."""
        self._strategy = strategy

    # ============== Product Level Methods ==============

    def get_satellite_product(self, product_id: str) -> Optional[dict]:
        """Get satellite product configuration."""
        product = self.SATELLITE_PRODUCTS.get(product_id)
        if not product:
            return None

        # Build instruments summary
        instruments_summary = {}
        for inst_id, inst_config in product.get("instruments", {}).items():
            instruments_summary[inst_id] = {
                "name": inst_config["name"],
                "description": inst_config["description"],
                "available": inst_config.get("available", False),
            }

        return {
            "product_id": product_id,
            "product_info": {
                "name": product["name"],
                "description": product["description"],
                "type": product["type"],
                "instruments": instruments_summary,
            },
            "endpoints": {
                inst_id: f"/products/{product_id}/{inst_id}"
                for inst_id in product.get("instruments", {}).keys()
            },
        }

    # ============== Instrument Level Methods ==============

    def instrument_exists(self, product_id: str, instrument_id: str) -> bool:
        """Check if an instrument exists for a product."""
        product = self.SATELLITE_PRODUCTS.get(product_id)
        if not product:
            return False
        return instrument_id in product.get("instruments", {})

    def get_instrument(self, product_id: str, instrument_id: str) -> Optional[dict]:
        """Get instrument configuration."""
        product = self.SATELLITE_PRODUCTS.get(product_id)
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
                "available": ch_config.get("available", False),
            }

        return {
            "product": product_id,
            "instrument": instrument_id,
            "instrument_info": {
                "name": instrument["name"],
                "description": instrument["description"],
                "channels": channels_summary,
            },
            "endpoints": {
                ch_id: f"/products/{product_id}/{instrument_id}/{ch_id}"
                for ch_id, ch_config in instrument.get("channels", {}).items()
                if ch_config.get("available", False)
            },
        }

    # ============== Channel Level Methods ==============

    def channel_exists(
        self, product_id: str, instrument_id: str, channel_id: str
    ) -> bool:
        """Check if a channel exists for an instrument."""
        product = self.SATELLITE_PRODUCTS.get(product_id)
        if not product:
            return False

        instrument = product.get("instruments", {}).get(instrument_id)
        if not instrument:
            return False

        return channel_id in instrument.get("channels", {})

    def get_channel_config(
        self, product_id: str, instrument_id: str, channel_id: str
    ) -> Optional[dict]:
        """Get full channel configuration including zoom levels and bounding box."""
        product = self.SATELLITE_PRODUCTS.get(product_id)
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
                "tile_format": "webp",
            }

        # Default configuration for other products
        return {
            "name": channel["name"],
            "description": channel["description"],
            "zoom_levels": {"min": 1, "max": 10},
            "bounding_box": {"minx": -180, "miny": -90, "maxx": 180, "maxy": 90},
            "tile_format": "webp",
        }

    async def get_channel_tilesets(
        self, product_id: str, instrument_id: str, channel_id: str
    ) -> dict:
        """Get available tilesets for a channel with full metadata."""
        channel_config = self.get_channel_config(product_id, instrument_id, channel_id)
        tilesets = await self._get_tilesets_for_channel(
            product_id, instrument_id, channel_id
        )

        tile_url_pattern = (
            f"/products/{product_id}/{instrument_id}/{channel_id}"
            "/{{tileset_id}}/{{z}}/{{x}}/{{y}}.webp"
        )

        return {
            "product": product_id,
            "instrument": instrument_id,
            "channel": channel_id,
            "channel_info": channel_config,
            "tilesets": tilesets,
            "tile_url_pattern": tile_url_pattern,
        }

    async def _get_tilesets_for_channel(
        self, product_id: str, instrument_id: str, channel_id: str
    ) -> List[dict]:
        """Get list of available tilesets for a channel via the sync strategy."""
        if not self._strategy:
            logger.warning("No sync strategy configured for tileset listing")
            return []

        dir_name = self.CHANNEL_DIR_MAPPING.get(channel_id, channel_id)
        tileset_ids = await self._strategy.get_tilesets(dir_name)

        tilesets = [
            {
                "id": tileset_id,
                "url_pattern": (
                    f"/products/{product_id}/{instrument_id}/{channel_id}"
                    "/{tileset_id}/{z}/{x}/{y}.webp"
                ),
            }
            for tileset_id in tileset_ids
        ]

        logger.info(
            "Found %d tilesets for %s/%s/%s",
            len(tilesets),
            product_id,
            instrument_id,
            channel_id,
        )
        return tilesets

    # ============== Tile Serving Methods ==============

    async def get_tile_data(  # pylint: disable=too-many-positional-arguments
        self,
        product_id: str,
        instrument_id: str,
        channel_id: str,
        tileset_id: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,unused-argument
        """Get tile data via the sync strategy."""
        if not self._strategy:
            return None

        dir_name = self.CHANNEL_DIR_MAPPING.get(channel_id, channel_id)
        return await self._strategy.get_tile(dir_name, tileset_id, z, x, y)

    def validate_zoom_level(
        self, product_id: str, instrument_id: str, channel_id: str, z: int
    ) -> Tuple[bool, str]:
        """Validate if a zoom level is valid for a channel."""
        config = self.get_channel_config(product_id, instrument_id, channel_id)
        if not config:
            return False, f"Channel '{channel_id}' not found"

        zoom_levels = config["zoom_levels"]
        if z < zoom_levels["min"] or z > zoom_levels["max"]:
            return (
                False,
                f"Zoom level {z} not available. "
                f"Valid range: {zoom_levels['min']}-{zoom_levels['max']}",
            )

        return True, ""

    # ============== Caching Helper Methods ==============

    def get_config_hash(self, config: dict) -> str:
        """Generate a stable hash for a configuration dictionary."""
        config_str = json.dumps(config, sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()

    def get_latest_tileset_timestamp(self, tilesets: List[dict]) -> Optional[datetime]:
        """Get the timestamp of the most recent tileset as a datetime object."""
        if not tilesets:
            return None

        sorted_tilesets = sorted(tilesets, key=lambda x: x["id"], reverse=True)

        if not sorted_tilesets:
            return None

        latest_id = sorted_tilesets[0]["id"]
        match = re.search(r"_s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})", latest_id)

        if match:
            year, julian_day, hour, minute, second = map(int, match.groups())
            dt = datetime.strptime(
                f"{year}{julian_day:03d}{hour:02d}{minute:02d}{second:02d}",
                "%Y%j%H%M%S",
            )
            return dt.replace(tzinfo=timezone.utc)

        return None


# Singleton instance
satellite_service = SatelliteService()
