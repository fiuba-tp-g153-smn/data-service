"""Satellite-specific endpoints (GOES-19, ABI, etc.)."""

from fastapi import APIRouter, HTTPException, status, Path as PathParam
from fastapi.responses import FileResponse

from dependencies import logger
from services.satellite_service import satellite_service
from models.satellite import (
    SatelliteProductResponse,
    InstrumentResponse,
    ChannelTilesetsResponse,
)

router = APIRouter(prefix="/products", tags=["Satellite"])


@router.get(
    "/{product_id}",
    status_code=status.HTTP_200_OK,
    summary="Get Satellite Product Details",
    response_description="Returns details for a specific satellite product",
    response_model=SatelliteProductResponse,
)
async def get_satellite_product(
    product_id: str = PathParam(
        ..., description="Satellite product identifier (e.g., goes-19)"
    )
):
    """
    Get detailed information about a specific satellite product.

    Returns available instruments and their endpoints.

    Example: /products/goes-19
    """
    product = satellite_service.get_satellite_product(product_id)
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Satellite product '{product_id}' not found",
        )

    logger.info(f"Getting satellite product details: {product_id}")
    return product


@router.get(
    "/{product_id}/{instrument_id}",
    status_code=status.HTTP_200_OK,
    summary="Get Instrument Details",
    response_description="Returns details for a specific instrument",
    response_model=InstrumentResponse,
)
async def get_instrument(
    product_id: str = PathParam(
        ..., description="Satellite product identifier (e.g., goes-19)"
    ),
    instrument_id: str = PathParam(
        ..., description="Instrument identifier (e.g., abi)"
    ),
):
    """
    Get detailed information about a specific instrument.

    Returns available channels and their endpoints.

    Example: /products/goes-19/abi
    """
    if not satellite_service.instrument_exists(product_id, instrument_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Instrument '{instrument_id}' not found for product '{product_id}'",
        )

    logger.info(f"Getting instrument details: {product_id}/{instrument_id}")
    return satellite_service.get_instrument(product_id, instrument_id)


@router.get(
    "/{product_id}/{instrument_id}/{channel_id}",
    status_code=status.HTTP_200_OK,
    summary="List Channel Tilesets",
    response_description="Returns available tilesets for a channel with metadata",
    response_model=ChannelTilesetsResponse,
)
async def list_channel_tilesets(
    product_id: str = PathParam(
        ..., description="Satellite product identifier (e.g., goes-19)"
    ),
    instrument_id: str = PathParam(
        ..., description="Instrument identifier (e.g., abi)"
    ),
    channel_id: str = PathParam(..., description="Channel identifier (e.g., ch-13)"),
):
    """
    List available tilesets for a specific channel.

    Returns:
    - Channel configuration (name, description, zoom levels, bounding box)
    - List of available tilesets with URL patterns
    - Tile URL pattern for fetching tiles

    Example: /products/goes-19/abi/ch-13
    """
    if not satellite_service.channel_exists(product_id, instrument_id, channel_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Channel '{channel_id}' not found for {product_id}/{instrument_id}",
        )

    logger.info(f"Listing tilesets for: {product_id}/{instrument_id}/{channel_id}")
    return satellite_service.get_channel_tilesets(product_id, instrument_id, channel_id)


@router.get(
    "/{product_id}/{instrument_id}/{channel_id}/{tileset_id}/{z}/{x}/{y}.webp",
    status_code=status.HTTP_200_OK,
    summary="Get Satellite Tile",
    response_description="Returns a specific satellite tile image",
    response_class=FileResponse,
)
async def get_satellite_tile(
    product_id: str = PathParam(
        ..., description="Satellite product identifier (e.g., goes-19)"
    ),
    instrument_id: str = PathParam(
        ..., description="Instrument identifier (e.g., abi)"
    ),
    channel_id: str = PathParam(..., description="Channel identifier (e.g., ch-13)"),
    tileset_id: str = PathParam(
        ..., description="Tileset identifier (timestamp-based)"
    ),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate"),
):
    """
    Serve a specific tile for a satellite product/instrument/channel.

    Example: /products/goes-19/abi/ch-13/OR_ABI-L1b-RadF-M6C13_G19_s20261234567/5/10/15.webp
    """
    # Validate channel
    if not satellite_service.channel_exists(product_id, instrument_id, channel_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Channel '{channel_id}' not found for {product_id}/{instrument_id}",
        )

    # Validate zoom level
    is_valid, error_msg = satellite_service.validate_zoom_level(
        product_id, instrument_id, channel_id, z
    )
    if not is_valid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_msg)

    # Get tile path
    tile_path = satellite_service.get_tile_path(
        product_id, instrument_id, channel_id, tileset_id, z, x, y
    )

    if not tile_path.exists():
        logger.warning(f"Satellite tile not found: {tile_path}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Tile not found"
        )

    logger.debug(f"Serving satellite tile: {tile_path}")
    return FileResponse(
        tile_path,
        media_type="image/webp",
        headers={
            "Cache-Control": "public, max-age=3600",
            "Access-Control-Allow-Origin": "*",
        },
    )
