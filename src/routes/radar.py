"""Radar-specific endpoints for the products API."""

from fastapi import APIRouter, HTTPException, Query
from fastapi import Path as PathParam
from fastapi import Request, Response, status

from dependencies import logger, settings
from models.radar import RadarPointValueResponse
from routes.utils import create_tile_response, make_transparent_tile_response
from services.point_value_service import CogNotFoundError, NoDataOrOutsideError
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
    return await radar_service.list_radars()


@router.get(
    "/{radar_id}",
    status_code=status.HTTP_200_OK,
    summary="List variables for a radar",
    response_description="Returns all available variables for a radar",
)
async def list_radar_variables(
    radar_id: str = PathParam(..., description="Radar identifier (e.g., RMA1, RMA2)")
):
    """List all variables for a given radar."""
    logger.info("API: Listing variables for radar: %s", radar_id)
    return await radar_service.list_radar_variables(radar_id)


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
    """List all elevations for a given radar variable."""
    logger.info(
        "API: Listing elevations for radar: %s, variable: %s", radar_id, variable_id
    )
    return await radar_service.list_radar_elevations(radar_id, variable_id)


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
    """List all tilesets for a radar variable and elevation."""
    logger.info(
        "API: Listing tilesets for radar: %s, variable: %s, elevation: %s",
        radar_id,
        variable_id,
        elevation_id,
    )
    return await radar_service.list_radar_tilesets(radar_id, variable_id, elevation_id)


@router.get(
    "/{radar_id}/{variable_id}/{elevation_id}/{tileset_id}/{z}/{x}/{y}.webp",
    status_code=status.HTTP_200_OK,
    summary="Get Radar Tile",
    response_description="Returns a specific radar tile image",
)
async def get_radar_tile(
    request: Request,
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
    # pylint: disable=too-many-arguments,disable=too-many-positional-arguments
    """Get Radar Tile."""
    # ETag based on unique tile identifier
    etag = f'"{radar_id}-{variable_id}-{elevation_id}-{tileset_id}-{z}-{x}-{y}"'

    # Check If-None-Match for 304
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    tile_data = await radar_service.get_tile_data(
        radar_id, variable_id, elevation_id, tileset_id, z, x, y
    )
    if not tile_data:
        logger.warning(
            "Radar tile not found, returning transparent fallback: %s/%s/%s/%s/%s/%s/%s",
            radar_id,
            variable_id,
            elevation_id,
            tileset_id,
            z,
            x,
            y,
        )
        return make_transparent_tile_response(etag, settings.cache_control_tile)

    logger.debug("Serving radar tile: %s/%s/%s/%s/%s", radar_id, tileset_id, z, x, y)

    return create_tile_response(tile_data, etag, settings.cache_control_tile)


@router.get(
    "/{radar_id}/{variable_id}/{elevation_id}/{tileset_id}/point",
    status_code=status.HTTP_200_OK,
    summary="Get Radar Point Value",
    response_description="Returns nearest sampled value for a lat/lon from radar COG",
    response_model=RadarPointValueResponse,
)
async def get_radar_point_value(
    radar_id: str = PathParam(..., description="Radar identifier (e.g., RMA1)"),
    variable_id: str = PathParam(..., description="Variable identifier (e.g., DBZH)"),
    elevation_id: str = PathParam(
        ..., description="Elevation identifier (e.g., elev0, elev1, elev2)"
    ),
    tileset_id: str = PathParam(..., description="Tileset identifier"),
    lat: float = Query(..., ge=-90.0, le=90.0, description="Latitude in EPSG:4326"),
    lon: float = Query(..., ge=-180.0, le=180.0, description="Longitude in EPSG:4326"),
):
    """Query nearest point value for a radar COG by geographic coordinate."""
    try:
        sample = await radar_service.get_point_value(
            radar_id=radar_id,
            variable_id=variable_id,
            elevation_id=elevation_id,
            tileset_id=tileset_id,
            lat=lat,
            lon=lon,
        )
    except CogNotFoundError as exc:
        logger.warning(
            "Radar COG not found for point query: %s/%s/%s/%s",
            radar_id,
            variable_id,
            elevation_id,
            tileset_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="cog_not_found",
        ) from exc
    except NoDataOrOutsideError as exc:
        logger.warning(
            "Radar point query returned nodata/outside: %s/%s/%s/%s lat=%s lon=%s",
            radar_id,
            variable_id,
            elevation_id,
            tileset_id,
            lat,
            lon,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="nodata_or_outside",
        ) from exc

    return {
        "radar": radar_id,
        "variable": variable_id,
        "elevation": elevation_id,
        "tileset_id": tileset_id,
        "lat": lat,
        "lon": lon,
        "value": sample.value,
        "unit": sample.unit,
    }
