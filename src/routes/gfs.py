"""GFS model product endpoints.

One router serves the three products (`mslp`, `500hpa`, `250hpa`): they share
the same cycle/step shape and differ only in which layers they carry, which the
listing endpoints report per product.
"""

import hashlib
import json
from typing import Tuple

from fastapi import APIRouter, HTTPException, Query
from fastapi import Path as PathParam
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse

from dependencies import logger, settings
from models.gfs import GfsCycleListResponse, GfsPointValueResponse, GfsStepListResponse
from routes.utils import create_tile_response, make_transparent_tile_response
from services.gfs_config import GFS_BARB_ZOOM_LEVELS, GFS_ZOOM_MAX, GFS_ZOOM_MIN
from services.gfs_service import gfs_service
from services.point_value_service import (
    CogNotFoundError,
    NoDataOrOutsideError,
    point_value_service,
)

router = APIRouter(prefix="/products/gfs", tags=["GFS Model"])

_PRODUCT_DESC = "GFS product ID: mslp, 500hpa or 250hpa"
_CYCLE_DESC = "Model run timestamp (e.g. 20260808T0000Z)"
_STEP_DESC = "Forecast step (e.g. f003)"


def _etag(payload: dict) -> str:
    return (
        f'"{hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()}"'
    )


def _etag_pair(identity: str) -> Tuple[str, str]:
    """`(hit, miss)` ETags for one tile identity.

    They must differ. A gap and the object that later fills it share a URL, so a
    single ETag would make the client's revalidation match its own cached gap
    and answer 304 forever — the tile would never arrive.
    """
    return f'"{identity}"', f'"{identity}-miss"'


def _not_modified(cache_control: str) -> Response:
    """304 that restates the Cache-Control, so a gap keeps its short freshness."""
    return Response(
        status_code=status.HTTP_304_NOT_MODIFIED,
        headers={"Cache-Control": cache_control},
    )


def _json_or_304(request: Request, payload: dict) -> Response:
    """Serve a listing with an ETag, or 304 when the client already has it."""
    etag = _etag(payload)
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)
    return JSONResponse(
        content=payload,
        headers={"Cache-Control": settings.cache_control_config, "ETag": etag},
    )


@router.get(
    "/{product_id}",
    status_code=status.HTTP_200_OK,
    summary="List GFS Cycles",
    response_model=GfsCycleListResponse,
)
async def list_cycles(
    request: Request,
    product_id: str = PathParam(..., description=_PRODUCT_DESC),
):
    """List available model runs for a GFS product."""
    data = await gfs_service.list_cycles(product_id)
    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown GFS product '{product_id}'",
        )
    return _json_or_304(request, data.model_dump())


@router.get(
    "/{product_id}/{cycle}",
    status_code=status.HTTP_200_OK,
    summary="List GFS Forecast Steps",
    response_model=GfsStepListResponse,
)
async def list_steps(
    request: Request,
    product_id: str = PathParam(..., description=_PRODUCT_DESC),
    cycle: str = PathParam(..., description=_CYCLE_DESC),
):
    """List the forecast steps of one GFS cycle, with their valid timestamps."""
    data = await gfs_service.list_steps(product_id, cycle)
    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cycle '{cycle}' not found for product '{product_id}'",
        )
    return _json_or_304(request, data.model_dump())


@router.get(
    "/{product_id}/{cycle}/{fxxx}/point",
    status_code=status.HTTP_200_OK,
    summary="Get GFS Point Value",
    response_model=GfsPointValueResponse,
)
async def get_point_value(
    product_id: str = PathParam(..., description=_PRODUCT_DESC),
    cycle: str = PathParam(..., description=_CYCLE_DESC),
    fxxx: str = PathParam(..., description=_STEP_DESC),
    lat: float = Query(..., ge=-90.0, le=90.0, description="Latitude in EPSG:4326"),
    lon: float = Query(..., ge=-180.0, le=180.0, description="Longitude in EPSG:4326"),
):
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Sample the nearest value from the GFS COG at a lat/lon point.

    Returns hPa for `mslp` and knots for the isobaric levels.
    """
    try:
        sample = await point_value_service.sample_gfs_point(
            product_id, cycle, fxxx, lat, lon
        )
    except CogNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="cog_not_found"
        ) from exc
    except NoDataOrOutsideError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="nodata_or_outside"
        ) from exc

    return GfsPointValueResponse(
        product_id=product_id,
        cycle=cycle,
        fxxx=fxxx,
        lat=lat,
        lon=lon,
        value=sample.value,
        unit=sample.unit,
    )


@router.get(
    "/{product_id}/{cycle}/{fxxx}/barbs/{z}/{x}/{y}.json",
    status_code=status.HTTP_200_OK,
    summary="Get GFS Wind-Barb GeoJSON Tile",
)
async def get_barb_tile(
    request: Request,
    product_id: str = PathParam(..., description=_PRODUCT_DESC),
    cycle: str = PathParam(..., description=_CYCLE_DESC),
    fxxx: str = PathParam(..., description=_STEP_DESC),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate"),
):
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Serve one wind-barb GeoJSON tile (500 hPa only).

    Native zooms are 2/4/6/8; above the deepest one the frontend overzooms the
    z8 tiles, which carry identical barbs.
    """
    if z not in GFS_BARB_ZOOM_LEVELS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Zoom {z} not supported for barb tiles. "
            f"Valid: {sorted(GFS_BARB_ZOOM_LEVELS)}",
        )

    etag, miss_etag = _etag_pair(f"{product_id}-{cycle}-{fxxx}-barbs-{z}-{x}-{y}")
    if_none_match = request.headers.get("if-none-match")
    if if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    data = await gfs_service.get_barb_tile(product_id, cycle, fxxx, z, x, y)
    if not data:
        # Empty FeatureCollection rather than 404: most tiles of a viewport hold
        # no barbs, and a 404 per tile would fill the browser console with noise
        # for a completely normal case. Its own ETag + short TTL, so a tile that
        # gains barbs later is not masked by the client's cached empty one.
        if if_none_match == miss_etag:
            return _not_modified(settings.gfs_cache_control_tile_miss)
        return Response(
            content=b'{"type":"FeatureCollection","features":[]}',
            media_type="application/geo+json",
            headers={
                "Cache-Control": settings.gfs_cache_control_tile_miss,
                "ETag": miss_etag,
            },
        )

    return Response(
        content=data,
        media_type="application/geo+json",
        headers={"Cache-Control": settings.cache_control_tile, "ETag": etag},
    )


@router.get(
    "/{product_id}/{cycle}/{fxxx}/{layer}.json",
    status_code=status.HTTP_200_OK,
    summary="Get GFS Overlay GeoJSON",
)
async def get_geojson_layer(
    request: Request,
    product_id: str = PathParam(..., description=_PRODUCT_DESC),
    cycle: str = PathParam(..., description=_CYCLE_DESC),
    fxxx: str = PathParam(..., description=_STEP_DESC),
    layer: str = PathParam(
        ..., description="Layer name: isobars, thickness, heights or isotherms"
    ),
):
    """Serve a contour overlay for a forecast step.

    Barbs are not reachable here: they are per-tile and have their own route.
    """
    etag = f'"{product_id}-{cycle}-{fxxx}-{layer}"'
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    data = await gfs_service.get_geojson(product_id, cycle, fxxx, layer)
    if not data:
        logger.warning(
            "GFS GeoJSON not found: %s/%s/%s/%s", product_id, cycle, fxxx, layer
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="GeoJSON not found"
        )

    return Response(
        content=data,
        media_type="application/geo+json",
        headers={"Cache-Control": settings.cache_control_tile, "ETag": etag},
    )


@router.get(
    "/{product_id}/{cycle}/{fxxx}/{z}/{x}/{y}.webp",
    status_code=status.HTTP_200_OK,
    summary="Get GFS Tile",
)
async def get_tile(
    request: Request,
    product_id: str = PathParam(..., description=_PRODUCT_DESC),
    cycle: str = PathParam(..., description=_CYCLE_DESC),
    fxxx: str = PathParam(..., description=_STEP_DESC),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate"),
):
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Serve one WebP tile of a GFS forecast step.

    `mslp` is contour-only and has no raster, so it always answers transparent.
    """
    if z < GFS_ZOOM_MIN or z > GFS_ZOOM_MAX:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Zoom level {z} not available. "
            f"Valid range: {GFS_ZOOM_MIN}-{GFS_ZOOM_MAX}",
        )

    etag, miss_etag = _etag_pair(f"{product_id}-{cycle}-{fxxx}-{z}-{x}-{y}")
    if_none_match = request.headers.get("if-none-match")
    if if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    tile_data = await gfs_service.get_tile_data(product_id, cycle, fxxx, z, x, y)
    if not tile_data:
        # Two different gaps land here. gdal2tiles only emits tiles the model
        # covers, so a hole at the domain edge is permanent; but a step is
        # advertised as soon as its COG lands, so a hole can also just mean the
        # pyramid is still being written. A transparent tile keeps Leaflet quiet
        # instead of tripping the frontend's "layer down" handler, and its own
        # ETag + short TTL keep the second case from being cached as the first.
        if if_none_match == miss_etag:
            return _not_modified(settings.gfs_cache_control_tile_miss)
        return make_transparent_tile_response(
            miss_etag, settings.gfs_cache_control_tile_miss
        )

    return create_tile_response(tile_data, etag, settings.cache_control_tile)
