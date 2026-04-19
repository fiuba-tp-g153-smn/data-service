"""Satellite-specific endpoints (GOES-19, ABI, etc.)."""

from email.utils import format_datetime

from fastapi import APIRouter, HTTPException, Query
from fastapi import Path as PathParam
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse

from dependencies import logger, settings
from routes.utils import create_tile_response
from models.satellite import (
    ChannelTilesetsResponse,
    InstrumentResponse,
    SatellitePointValueResponse,
    SatelliteProductResponse,
)
from services.point_value_service import CogNotFoundError, NoDataOrOutsideError
from services.satellite_service import satellite_service

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

    logger.info("Getting satellite product details: %s", product_id)
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

    logger.info("Getting instrument details: %s/%s", product_id, instrument_id)
    return satellite_service.get_instrument(product_id, instrument_id)


@router.get(
    "/{product_id}/{instrument_id}/{channel_id}",
    status_code=status.HTTP_200_OK,
    summary="List Channel Tilesets",
    response_description="Returns available tilesets for a channel with metadata",
    response_model=ChannelTilesetsResponse,
)
async def list_channel_tilesets(
    request: Request,
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

    logger.info("Listing tilesets for: %s/%s/%s", product_id, instrument_id, channel_id)
    data = await satellite_service.get_channel_tilesets(
        product_id, instrument_id, channel_id
    )

    # Generate ETag
    etag = f'"{satellite_service.get_config_hash(data)}"'

    # Check If-None-Match for 304
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    # Get Last-Modified
    last_modified = satellite_service.get_latest_tileset_timestamp(data["tilesets"])

    headers = {
        "Cache-Control": settings.cache_control_config,
        "ETag": etag,
    }

    if last_modified:
        headers["Last-Modified"] = format_datetime(last_modified, usegmt=True)

    return JSONResponse(content=data, headers=headers)


@router.get(
    "/{product_id}/{instrument_id}/{channel_id}/{tileset_id}/{z}/{x}/{y}.webp",
    status_code=status.HTTP_200_OK,
    summary="Get Satellite Tile",
    response_description="Returns a specific satellite tile image",
)
async def get_satellite_tile(
    request: Request,
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
    # pylint: disable=too-many-arguments,disable=too-many-positional-arguments
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

    # ETag based on unique tile identifier
    etag = f'"{tileset_id}-{z}-{x}-{y}"'

    # Check If-None-Match for 304
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    # Get tile data from Redis
    tile_data = await satellite_service.get_tile_data(
        product_id, instrument_id, channel_id, tileset_id, z, x, y
    )

    if not tile_data:
        logger.warning("Satellite tile not found: %s/%s/%s/%s", tileset_id, z, x, y)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Tile not found"
        )

    logger.debug("Serving satellite tile: %s/%s/%s/%s", tileset_id, z, x, y)

    return create_tile_response(tile_data, etag, settings.cache_control_tile)


@router.get(
    "/{product_id}/{instrument_id}/{channel_id}/{tileset_id}/point",
    status_code=status.HTTP_200_OK,
    summary="Get Satellite Point Value",
    response_description="Returns nearest sampled value for a lat/lon from satellite COG",
    response_model=SatellitePointValueResponse,
)
async def get_satellite_point_value(
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
    lat: float = Query(..., ge=-90.0, le=90.0, description="Latitude in EPSG:4326"),
    lon: float = Query(..., ge=-180.0, le=180.0, description="Longitude in EPSG:4326"),
):
    """Query nearest point value for a satellite COG by geographic coordinate."""
    if not satellite_service.channel_exists(product_id, instrument_id, channel_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Channel '{channel_id}' not found for {product_id}/{instrument_id}",
        )

    try:
        sample = await satellite_service.get_point_value(
            product_id=product_id,
            instrument_id=instrument_id,
            channel_id=channel_id,
            tileset_id=tileset_id,
            lat=lat,
            lon=lon,
        )
    except CogNotFoundError as exc:
        logger.warning(
            "Satellite COG not found for point query: %s/%s/%s/%s",
            product_id,
            instrument_id,
            channel_id,
            tileset_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="cog_not_found",
        ) from exc
    except NoDataOrOutsideError as exc:
        logger.warning(
            "Satellite point query returned nodata/outside: %s/%s/%s/%s lat=%s lon=%s",
            product_id,
            instrument_id,
            channel_id,
            tileset_id,
            lat,
            lon,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="nodata_or_outside",
        ) from exc

    return {
        "product": product_id,
        "instrument": instrument_id,
        "channel": channel_id,
        "tileset_id": tileset_id,
        "lat": lat,
        "lon": lon,
        "value": sample.value,
        "unit": sample.unit,
    }
