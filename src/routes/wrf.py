"""WRF model product endpoints."""

import hashlib
import json

from fastapi import APIRouter, HTTPException, Query
from fastapi import Path as PathParam
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse

from dependencies import logger, settings
from models.wrf import (
    WrfInitRunListResponse,
    WrfPointValueResponse,
    WrfStepListResponse,
)
from routes.utils import create_tile_response, make_transparent_tile_response
from services.point_value_service import (
    CogNotFoundError,
    NoDataOrOutsideError,
    point_value_service,
)
from services.wrf_service import wrf_service

router = APIRouter(prefix="/products/wrf", tags=["WRF Model"])

_ZOOM_MIN = 4
_ZOOM_MAX = 9

_BARB_ZOOM_LEVELS = {2, 4, 6, 8, 10, 12}


@router.get(
    "/{product_id}",
    status_code=status.HTTP_200_OK,
    summary="List WRF Initialization Runs",
    response_model=WrfInitRunListResponse,
)
async def list_init_runs(
    request: Request,
    product_id: str = PathParam(..., description="WRF product ID (e.g. Colmax)"),
):
    """List available initialization runs for a WRF product."""
    data = await wrf_service.list_init_runs(product_id)
    payload = data.model_dump()
    etag = (
        f'"{hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()}"'
    )

    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    return JSONResponse(
        content=payload,
        headers={"Cache-Control": settings.cache_control_config, "ETag": etag},
    )


@router.get(
    "/{product_id}/{init_tag}",
    status_code=status.HTTP_200_OK,
    summary="List WRF Forecast Steps",
    response_model=WrfStepListResponse,
)
async def list_steps(
    request: Request,
    product_id: str = PathParam(..., description="WRF product ID"),
    init_tag: str = PathParam(
        ..., description="Initialization timestamp (e.g. 20260430_060000)"
    ),
):
    """List all forecast steps for a WRF product initialization run."""
    data = await wrf_service.list_steps(product_id, init_tag)
    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Init run '{init_tag}' not found for product '{product_id}'",
        )

    payload = data.model_dump()
    etag = (
        f'"{hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()}"'
    )

    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    return JSONResponse(
        content=payload,
        headers={"Cache-Control": settings.cache_control_config, "ETag": etag},
    )


@router.get(
    "/{product_id}/{init_tag}/{fxxx}/point",
    status_code=status.HTTP_200_OK,
    summary="Get WRF Point Value",
    response_model=WrfPointValueResponse,
)
async def get_point_value(
    product_id: str = PathParam(..., description="WRF product ID"),
    init_tag: str = PathParam(..., description="Initialization timestamp"),
    fxxx: str = PathParam(..., description="Forecast step (e.g. F001)"),
    lat: float = Query(..., ge=-90.0, le=90.0, description="Latitude in EPSG:4326"),
    lon: float = Query(..., ge=-180.0, le=180.0, description="Longitude in EPSG:4326"),
):
    """Sample nearest value from the WRF primary-field COG at a lat/lon point."""
    try:
        sample = await point_value_service.sample_wrf_point(
            product_id, init_tag, fxxx, lat, lon
        )
    except CogNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="cog_not_found"
        ) from exc
    except NoDataOrOutsideError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="nodata_or_outside"
        ) from exc

    return WrfPointValueResponse(
        product_id=product_id,
        init_tag=init_tag,
        fxxx=fxxx,
        lat=lat,
        lon=lon,
        value=sample.value,
        unit=sample.unit,
    )


@router.get(
    "/{product_id}/{init_tag}/{fxxx}/{layer}.json",
    status_code=status.HTTP_200_OK,
    summary="Get WRF Overlay GeoJSON",
)
async def get_geojson_layer(
    request: Request,
    product_id: str = PathParam(..., description="WRF product ID"),
    init_tag: str = PathParam(..., description="Initialization timestamp"),
    fxxx: str = PathParam(..., description="Forecast step"),
    layer: str = PathParam(..., description="Layer name (e.g. barbs, slp, brn)"),
):
    """Serve a WRF overlay GeoJSON (barbs / contours) for a forecast step."""
    etag = f'"{product_id}-{init_tag}-{fxxx}-{layer}"'
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    data = await wrf_service.get_geojson(product_id, init_tag, fxxx, layer)
    if not data:
        logger.warning(
            "WRF GeoJSON not found: %s/%s/%s/%s", product_id, init_tag, fxxx, layer
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
    "/{product_id}/{init_tag}/{fxxx}/barbs/{z}/{x}/{y}.json",
    status_code=status.HTTP_200_OK,
    summary="Get WRF Wind-Barb GeoJSON Tile",
)
async def get_barb_tile(
    request: Request,
    product_id: str = PathParam(..., description="WRF product ID"),
    init_tag: str = PathParam(..., description="Initialization timestamp"),
    fxxx: str = PathParam(..., description="Forecast step (e.g. F001)"),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate"),
):
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Serve a wind-barb GeoJSON tile (native zooms: 2, 4, 6, 8, 10)."""
    if z not in _BARB_ZOOM_LEVELS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Zoom {z} not supported for barb tiles. "
            f"Valid: {sorted(_BARB_ZOOM_LEVELS)}",
        )

    etag = f'"{product_id}-{init_tag}-{fxxx}-barbs-{z}-{x}-{y}"'
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    data = await wrf_service.get_barb_tile(product_id, init_tag, fxxx, z, x, y)
    if not data:
        # Return empty FeatureCollection (not 404) so the browser console stays
        # clean for tiles outside the WRF Lambert domain.
        return Response(
            content=b'{"type":"FeatureCollection","features":[]}',
            media_type="application/geo+json",
            headers={"Cache-Control": settings.cache_control_tile, "ETag": etag},
        )

    return Response(
        content=data,
        media_type="application/geo+json",
        headers={"Cache-Control": settings.cache_control_tile, "ETag": etag},
    )


@router.get(
    "/{product_id}/{init_tag}/{fxxx}/{z}/{x}/{y}.webp",
    status_code=status.HTTP_200_OK,
    summary="Get WRF Tile",
)
async def get_tile(
    request: Request,
    product_id: str = PathParam(..., description="WRF product ID"),
    init_tag: str = PathParam(..., description="Initialization timestamp"),
    fxxx: str = PathParam(..., description="Forecast step (e.g. F001)"),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate"),
):
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Serve a single WebP tile for the given WRF forecast step."""
    if z < _ZOOM_MIN or z > _ZOOM_MAX:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Zoom level {z} not available. Valid range: {_ZOOM_MIN}-{_ZOOM_MAX}",
        )

    etag = f'"{product_id}-{init_tag}-{fxxx}-{z}-{x}-{y}"'
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    tile_data = await wrf_service.get_tile_data(product_id, init_tag, fxxx, z, x, y)
    if not tile_data:
        # WRF Lambert domain is curvilinear; gdal2tiles only emits tiles where
        # the model has data. Tiles outside that footprint legitimately do not
        # exist, so a 404 here is normal at the model boundary. Return a
        # transparent WebP instead so Leaflet renders empty pixels and the
        # frontend's tileerror handler does not surface a "layer down" toast.
        return make_transparent_tile_response(etag, settings.cache_control_tile)

    return create_tile_response(tile_data, etag, settings.cache_control_tile)
