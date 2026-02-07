"""Radar-specific endpoints for the products API."""

from fastapi import APIRouter, HTTPException
from fastapi import Path as PathParam
from fastapi import status
from fastapi.responses import FileResponse
from dependencies import logger
from services.radar_service import radar_service

router = APIRouter(prefix="/products/radar", tags=["Radar"])


@router.get(
    "",
    status_code=status.HTTP_200_OK,
    summary="List all radars",
    response_description="Returns all available radars",
)
async def list_radars():
    """
    List all available radars (e.g., RMA1, RMA2, ...)
    """
    logger.info("API: Listing all radars")
    return radar_service.list_radars()


@router.get(
    "/{radar_id}",
    status_code=status.HTTP_200_OK,
    summary="List variables for a radar",
    response_description="Returns all available variables for a radar",
)
async def list_radar_variables(
    radar_id: str = PathParam(..., description="Radar identifier (e.g., RMA1, RMA2)")
):
    logger.info(f"API: Listing variables for radar: {radar_id}")
    return radar_service.list_radar_variables(radar_id)


@router.get(
    "/{radar_id}/{variable_id}",
    status_code=status.HTTP_200_OK,
    summary="List elevations for a radar variable",
    response_description="Returns all available elevations for a radar variable",
)
async def list_radar_elevations(
    radar_id: str = PathParam(..., description="Radar identifier (e.g., RMA1)"),
    variable_id: str = PathParam(..., description="Variable identifier (e.g., DBZH)"),
):
    logger.info(
        f"API: Listing elevations for radar: {radar_id}, variable: {variable_id}"
    )
    return radar_service.list_radar_elevations(radar_id, variable_id)


@router.get(
    "/{radar_id}/{variable_id}/{elevation_id}",
    status_code=status.HTTP_200_OK,
    summary="List tilesets for a radar variable and elevation",
    response_description="Returns all available tilesets for a radar variable and elevation",
)
async def list_radar_tilesets(
    radar_id: str = PathParam(..., description="Radar identifier (e.g., RMA1)"),
    variable_id: str = PathParam(..., description="Variable identifier (e.g., DBZH)"),
    elevation_id: str = PathParam(
        ..., description="Elevation identifier (e.g., elev0, elev1, elev2)"
    ),
):
    logger.info(
        f"API: Listing tilesets for radar: {radar_id}, variable: {variable_id}, elevation: {elevation_id}"
    )
    return radar_service.list_radar_tilesets(radar_id, variable_id, elevation_id)


@router.get(
    "/{radar_id}/{variable_id}/{elevation_id}/{tileset_id}/{z}/{x}/{y}.webp",
    status_code=status.HTTP_200_OK,
    summary="Get Radar Tile",
    response_description="Returns a specific radar tile image",
    response_class=FileResponse,
)
async def get_radar_tile(
    radar_id: str = PathParam(..., description="Radar identifier (e.g., RMA1)"),
    variable_id: str = PathParam(..., description="Variable identifier (e.g., DBZH)"),
    elevation_id: str = PathParam(
        ..., description="Elevation identifier (e.g., elev0, elev1, elev2)"
    ),
    tileset_id: str = PathParam(
        ..., description="Tileset identifier (timestamp, e.g., 20260114T170328Z)"
    ),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate"),
):
    tile_path = radar_service.get_tile_path_output_radar(
        radar_id, variable_id, elevation_id, tileset_id, z, x, y
    )
    if not tile_path.exists():
        logger.warning(f"Radar tile not found: {tile_path}")
        raise HTTPException(status_code=404, detail="Tile not found")
    logger.debug(f"Serving radar tile: {tile_path}")
    return FileResponse(tile_path)
