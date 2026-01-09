"""Service for managing GOES satellite tiles."""
from pathlib import Path
from typing import Dict, List, Optional

from dependencies import logger


class TilesService:
    """Service to manage and serve GOES satellite tiles."""
    
    TILES_BASE_PATH = Path.cwd() / ".tmp"
    
    AVAILABLE_PRODUCTS: Dict[str, dict] = {
        "band_13": {
            "name": "Band 13 - Cloud Top",
            "zoom_levels": {"min": 3, "max": 7},
        },
        # "band_2": {
        #     "name": "Band 2 - Red Visible",
        #     "zoom_levels": {"min": 3, "max": 7},
        # },
        # "band_9": {
        #     "name": "Band 9 - Mid-Level Water Vapor",
        #     "zoom_levels": {"min": 3, "max": 7},
        # },
    }
    
    def get_products(self) -> dict:
        """Get all available products and their configuration."""
        return {
            "products": self.AVAILABLE_PRODUCTS,
            "tile_format": "webp",
            "tile_url_pattern": "/{product}/{tileset_id}/{z}/{x}/{y}.webp"
        }
    
    def product_exists(self, product: str) -> bool:
        """Check if a product exists."""
        return product in self.AVAILABLE_PRODUCTS
    
    def get_product_config(self, product: str) -> Optional[dict]:
        """Get configuration for a specific product."""
        return self.AVAILABLE_PRODUCTS.get(product)
    
    def get_tilesets(self, product: str) -> List[dict]:
        """
        Get available tilesets for a specific product.
        
        Args:
            product: Product identifier (e.g., band_13)
            
        Returns:
            List of tileset information dictionaries
        """
        tiles_dir = self.TILES_BASE_PATH / product / "tiles"
        logger.info(f"Looking for tilesets in directory: {self.TILES_BASE_PATH}")
        logger.info(f"Tiles directory exists: {tiles_dir}")
        
        if not tiles_dir.exists():
            logger.info(f"Tiles directory does not exist: {tiles_dir}")
            return []
        
        tilesets = []
        for item in tiles_dir.iterdir():
            if item.is_dir() and item.name.endswith("_tiles"):
                tileset_id = item.name.replace("_tiles", "")
                tilesets.append({
                    "id": tileset_id,
                    "url_pattern": f"/{product}/{tileset_id}/{{z}}/{{x}}/{{y}}.webp"
                })
        
        logger.info(f"Found {len(tilesets)} tilesets for product {product}")
        return tilesets
    
    def get_tile_path(self, product: str, tileset_id: str, z: int, x: int, y: int) -> Path:
        """
        Build the full path to a tile file.
        
        Args:
            product: Product identifier
            tileset_id: Tileset identifier (filename stem)
            z: Zoom level
            x: Tile X coordinate
            y: Tile Y coordinate
            
        Returns:
            Path to the tile file
        """
        return self.TILES_BASE_PATH / product / "tiles" / f"{tileset_id}_tiles" / str(z) / str(x) / f"{y}.webp"
    
    def validate_zoom_level(self, product: str, z: int) -> tuple[bool, str]:
        """
        Validate if a zoom level is valid for a product.
        
        Args:
            product: Product identifier
            z: Zoom level
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        config = self.get_product_config(product)
        if not config:
            return False, f"Product '{product}' not found"
        
        zoom_levels = config["zoom_levels"]
        if z < zoom_levels["min"] or z > zoom_levels["max"]:
            return False, f"Zoom level {z} not available. Valid range: {zoom_levels['min']}-{zoom_levels['max']}"
        
        return True, ""


tiles_service = TilesService()