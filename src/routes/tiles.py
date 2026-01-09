"""Routes for GOES satellite tiles endpoints."""
from fastapi import APIRouter, HTTPException, status, Path as PathParam
from fastapi.responses import FileResponse

from dependencies import logger
from services.tiles_service import tiles_service
from models.tiles import ProductsResponse, TilesetsResponse, TilesetInfo

router = APIRouter(prefix="/tiles", tags=["Tiles"])


@router.get(
    "/products",
    status_code=status.HTTP_200_OK,
    summary="List Available Products",
    response_description="Returns all available products/channels and their configuration",
    response_model=ProductsResponse
)
async def list_products():
    """
    List all available products/channels and their configuration.
    
    Products represent different satellite bands (e.g., band_13 for Cloud Top imagery).
    """
    logger.info("Listing all available tile products")
    return tiles_service.get_products()


@router.get(
    "/products/{product}/tilesets",
    status_code=status.HTTP_200_OK,
    summary="List Tilesets for Product",
    response_description="Returns available tilesets for a specific product",
    response_model=TilesetsResponse
)
async def list_tilesets(
    product: str = PathParam(..., description="Product identifier (e.g., band_13)")
):
    """
    List available tilesets for a specific product.
    
    Returns timestamps of available processed tiles.
    
    Example: /tiles/products/band_13/tilesets
    """
    if not tiles_service.product_exists(product):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Product '{product}' not found"
        )
    
    logger.info(f"Listing tilesets for product: {product}")
    tilesets = tiles_service.get_tilesets(product)
    
    return TilesetsResponse(
        product=product,
        product_info=tiles_service.get_product_config(product),
        tilesets=[TilesetInfo(**ts) for ts in tilesets]
    )


@router.get(
    "/{product}/{tileset_id}/{z}/{x}/{y}.webp",
    status_code=status.HTTP_200_OK,
    summary="Get Tile",
    response_description="Returns a specific tile image",
    response_class=FileResponse
)
async def get_tile(
    product: str = PathParam(..., description="Product identifier (e.g., band_13)"),
    tileset_id: str = PathParam(..., description="Tileset identifier (filename stem)"),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate")
):
    """
    Serve a specific tile for a product.
    
    Example: /tiles/band_13/OR_ABI-L1b-RadF-M6C13_G19_s20261234567/5/10/15.webp
    """
    # Validate product
    if not tiles_service.product_exists(product):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Product '{product}' not found"
        )
    
    # Validate zoom level
    is_valid, error_msg = tiles_service.validate_zoom_level(product, z)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_msg
        )
    
    # Get tile path
    tile_path = tiles_service.get_tile_path(product, tileset_id, z, x, y)
    
    if not tile_path.exists():
        logger.warning(f"Tile not found: {tile_path}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tile not found"
        )
    
    logger.debug(f"Serving tile: {tile_path}")
    return FileResponse(
        tile_path,
        media_type="image/webp",
        headers={
            "Cache-Control": "public, max-age=3600",
            "Access-Control-Allow-Origin": "*",
        }
    )