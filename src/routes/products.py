from fastapi import APIRouter, HTTPException, status, Path as PathParam
from fastapi.responses import FileResponse

from dependencies import logger
from services.products_service import products_service
from models.products import (
    ProductsListResponse,
    ProductResponse,
    InstrumentResponse,
    ChannelTilesetsResponse,
    TilesetInfo,
)

router = APIRouter(prefix="/products", tags=["Products"])

@router.get(
    "",
    status_code=status.HTTP_200_OK,
    summary="List All Products",
    response_description="Returns all available products (satellites, radar, models, etc.)",
    response_model=ProductsListResponse
)
async def list_products():
    """
    List all available products and their basic information.
    
    Products include:
    - **goes-19**: GOES-19 Satellite (ABI, GLM instruments)
    - **radar**: Weather Radar Network
    - **numerical-models**: Numerical Weather Prediction Models
    - **emas**: Automatic Weather Stations
    """
    logger.info("Listing all available products")
    return products_service.get_products_list()


@router.get(
    "/{product_id}",
    status_code=status.HTTP_200_OK,
    summary="Get Product Details",
    response_description="Returns details for a specific product",
    response_model=ProductResponse
)
async def get_product(
    product_id: str = PathParam(..., description="Product identifier (e.g., goes-19)")
):
    """
    Get detailed information about a specific product.
    
    Returns available instruments and their endpoints.
    
    Example: /products/goes-19
    """
    if not products_service.product_exists(product_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Product '{product_id}' not found"
        )
    
    logger.info(f"Getting product details: {product_id}")
    return products_service.get_product(product_id)


# ============== Instrument Level Endpoints ==============

@router.get(
    "/{product_id}/{instrument_id}",
    status_code=status.HTTP_200_OK,
    summary="Get Instrument Details",
    response_description="Returns details for a specific instrument",
    response_model=InstrumentResponse
)
async def get_instrument(
    product_id: str = PathParam(..., description="Product identifier (e.g., goes-19)"),
    instrument_id: str = PathParam(..., description="Instrument identifier (e.g., abi)")
):
    """
    Get detailed information about a specific instrument.
    
    Returns available channels and their endpoints.
    
    Example: /products/goes-19/abi
    """
    if not products_service.product_exists(product_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Product '{product_id}' not found"
        )
    
    if not products_service.instrument_exists(product_id, instrument_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Instrument '{instrument_id}' not found for product '{product_id}'"
        )
    
    logger.info(f"Getting instrument details: {product_id}/{instrument_id}")
    return products_service.get_instrument(product_id, instrument_id)


# ============== Channel Level Endpoints ==============

@router.get(
    "/{product_id}/{instrument_id}/{channel_id}",
    status_code=status.HTTP_200_OK,
    summary="List Channel Tilesets",
    response_description="Returns available tilesets for a channel with metadata",
    response_model=ChannelTilesetsResponse
)
async def list_channel_tilesets(
    product_id: str = PathParam(..., description="Product identifier (e.g., goes-19)"),
    instrument_id: str = PathParam(..., description="Instrument identifier (e.g., abi)"),
    channel_id: str = PathParam(..., description="Channel identifier (e.g., ch-13)")
):
    """
    List available tilesets for a specific channel.
    
    Returns:
    - Channel configuration (name, description, zoom levels, bounding box)
    - List of available tilesets with URL patterns
    - Tile URL pattern for fetching tiles
    
    Example: /products/goes-19/abi/ch-13
    """
    if not products_service.product_exists(product_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Product '{product_id}' not found"
        )
    
    if not products_service.instrument_exists(product_id, instrument_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Instrument '{instrument_id}' not found for product '{product_id}'"
        )
    
    if not products_service.channel_exists(product_id, instrument_id, channel_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Channel '{channel_id}' not found for {product_id}/{instrument_id}"
        )
    
    logger.info(f"Listing tilesets for: {product_id}/{instrument_id}/{channel_id}")
    return products_service.get_channel_tilesets(product_id, instrument_id, channel_id)


# ============== Tile Serving Endpoint ==============

@router.get(
    "/{product_id}/{instrument_id}/{channel_id}/{tileset_id}/{z}/{x}/{y}.webp",
    status_code=status.HTTP_200_OK,
    summary="Get Tile",
    response_description="Returns a specific tile image",
    response_class=FileResponse
)
async def get_tile(
    product_id: str = PathParam(..., description="Product identifier (e.g., goes-19)"),
    instrument_id: str = PathParam(..., description="Instrument identifier (e.g., abi)"),
    channel_id: str = PathParam(..., description="Channel identifier (e.g., ch-13)"),
    tileset_id: str = PathParam(..., description="Tileset identifier (timestamp-based)"),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate")
):
    """
    Serve a specific tile for a product/instrument/channel.
    
    Example: /products/goes-19/abi/ch-13/OR_ABI-L1b-RadF-M6C13_G19_s20261234567/5/10/15.webp
    """
    # Validate product
    if not products_service.product_exists(product_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Product '{product_id}' not found"
        )
    
    # Validate instrument
    if not products_service.instrument_exists(product_id, instrument_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Instrument '{instrument_id}' not found for product '{product_id}'"
        )
    
    # Validate channel
    if not products_service.channel_exists(product_id, instrument_id, channel_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Channel '{channel_id}' not found for {product_id}/{instrument_id}"
        )
    
    # Validate zoom level
    is_valid, error_msg = products_service.validate_zoom_level(product_id, instrument_id, channel_id, z)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_msg
        )
    
    # Get tile path
    tile_path = products_service.get_tile_path(product_id, instrument_id, channel_id, tileset_id, z, x, y)
    
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