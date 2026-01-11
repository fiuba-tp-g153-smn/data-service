"""Base service with shared functionality for all products."""
from pathlib import Path
from typing import Dict


class BaseProductService:
    """Base class for product services with shared functionality."""
    
    TILES_BASE_PATH = Path.cwd() / ".tmp"
    
    # All products registry
    PRODUCTS_REGISTRY: Dict[str, dict] = {}
    
    @classmethod
    def register_product(cls, product_id: str, config: dict):
        """Register a product in the global registry."""
        cls.PRODUCTS_REGISTRY[product_id] = config
    
    @classmethod
    def get_all_products(cls) -> dict:
        """Get summary of all registered products."""
        products_summary = {}
        for product_id, config in cls.PRODUCTS_REGISTRY.items():
            # For satellite products, check instruments
            if config.get("type") == "satellite":
                has_available = any(
                    inst.get("available", False) 
                    for inst in config.get("instruments", {}).values()
                )
            # For radar products, check if it has the registry flag
            elif product_id == "radar":
                has_available = config.get("available", False)
            else:
                has_available = config.get("available", False)
            
            products_summary[product_id] = {
                "name": config["name"],
                "description": config["description"],
                "type": config["type"],
                "available": has_available
            }
        return {"products": products_summary, "api_version": "1.0"}
    
    @classmethod
    def product_exists(cls, product_id: str) -> bool:
        """Check if a product exists in the registry."""
        return product_id in cls.PRODUCTS_REGISTRY
