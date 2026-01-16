"""Radar-specific endpoints for the products API."""

from fastapi import APIRouter, HTTPException, status, Path as PathParam
from fastapi.responses import FileResponse

from dependencies import logger
from services.radar_service import radar_service
from models.radar import (
    RadarProductResponse,
    RadarVariableResponse,
    RadarStationTilesetsResponse,
)

router = APIRouter(prefix="/products/radar", tags=["Radar"])


@router.get(
    "",
    status_code=status.HTTP_200_OK,
    summary="Get Radar Product Info",
    response_description="Returns radar product with available variables",
    response_model=RadarProductResponse,
)
async def get_radar_product():
    """
    Get radar product information with available meteorological variables.

    Variables include:
    - **dbzh**: Reflectividad Horizontal (dBZ)
    - **zdr**: Reflectividad Diferencial (dB)
    - **rhohv**: Coeficiente de Correlación
    - **kdp**: Fase Diferencial Específica (°/km)
    """
    logger.info("Getting radar product info")
    return radar_service.get_radar_product()


@router.get(
    "/{variable_id}",
    status_code=status.HTTP_200_OK,
    summary="Get Radar Variable Details",
    response_description="Returns details for a specific radar variable with available stations",
    response_model=RadarVariableResponse,
)
async def get_radar_variable(
    variable_id: str = PathParam(
        ..., description="Variable identifier (e.g., dbzh, zdr, rhohv, kdp)"
    )
):
    """
    Get detailed information about a specific radar variable.

    Returns:
    - Variable configuration (name, description, unit, zoom levels)
    - List of available radar stations
    - Available elevation angles
    - Endpoints for each station

    Example: /products/radar/dbzh
    """
    if not radar_service.variable_exists(variable_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Radar variable '{variable_id}' not found. Available: dbzh, zdr, rhohv, kdp",
        )

    logger.info(f"Getting radar variable details: {variable_id}")
    return radar_service.get_variable(variable_id)


@router.get(
    "/{variable_id}/{station_id}",
    status_code=status.HTTP_200_OK,
    summary="Get Radar Station Tilesets",
    response_description="Returns available tilesets for a radar station and variable",
    response_model=RadarStationTilesetsResponse,
)
async def get_radar_station_tilesets(
    variable_id: str = PathParam(..., description="Variable identifier (e.g., dbzh)"),
    station_id: str = PathParam(
        ..., description="Station identifier (e.g., rma3, rma4, rma9)"
    ),
):
    """
    Get available tilesets for a specific radar station and variable.

    Returns:
    - Station info (name)
    - Variable config (name, description, unit, zoom levels)
    - Available elevation angles
    - List of available tilesets (timestamps)
    - Tile URL pattern

    Example: /products/radar/dbzh/rma3
    """
    if not radar_service.variable_exists(variable_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Radar variable '{variable_id}' not found",
        )

    if not radar_service.station_exists(station_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Radar station '{station_id}' not found. Available: rma3, rma4, rma9",
        )

    logger.info(f"Getting radar station tilesets: {variable_id}/{station_id}")
    return radar_service.get_station_tilesets(variable_id, station_id)


# ============== Radar Tile Serving Endpoint ==============


@router.get(
    "/{variable_id}/{station_id}/{elevation_id}/{tileset_id}/{z}/{x}/{y}.webp",
    status_code=status.HTTP_200_OK,
    summary="Get Radar Tile",
    response_description="Returns a specific radar tile image",
    response_class=FileResponse,
)
async def get_radar_tile(
    variable_id: str = PathParam(..., description="Variable identifier (e.g., dbzh)"),
    station_id: str = PathParam(..., description="Station identifier (e.g., rma3)"),
    elevation_id: str = PathParam(
        ..., description="Elevation identifier (e.g., elev0, elev1, elev2)"
    ),
    tileset_id: str = PathParam(
        ..., description="Tileset identifier (timestamp, e.g., 20251230T151917Z)"
    ),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate"),
):
    """
    Serve a specific radar tile.

    Path structure: /products/radar/{variable}/{station}/{elevation}/{timestamp}/{z}/{x}/{y}.webp

    Example: /products/radar/dbzh/rma3/elev0/20251230T151917Z/10/332/432.webp

    Elevations:
    - elev0: 0.5°
    - elev1: 0.9°
    - elev2: 1.3°
    """
    # Validate variable
    if not radar_service.variable_exists(variable_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Radar variable '{variable_id}' not found",
        )

    # Validate station
    if not radar_service.station_exists(station_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Radar station '{station_id}' not found",
        )

    # Validate elevation
    if not radar_service.elevation_exists(elevation_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Elevation '{elevation_id}' not found. Available: elev0, elev1, elev2",
        )

    # Validate zoom level
    is_valid, error_msg = radar_service.validate_zoom_level(z)
    if not is_valid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_msg)

    # Get tile path
    tile_path = radar_service.get_tile_path(
        variable_id, station_id, elevation_id, tileset_id, z, x, y
    )

    if not tile_path.exists():
        logger.warning(f"Radar tile not found: {tile_path}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Tile not found"
        )

    logger.debug(f"Serving radar tile: {tile_path}")
    return FileResponse(
        tile_path,
        media_type="image/webp",
        headers={
            "Cache-Control": "public, max-age=300",  # Shorter cache for radar (more dynamic)
            "Access-Control-Allow-Origin": "*",
        },
    )
