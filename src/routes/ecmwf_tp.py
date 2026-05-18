"""ECMWF total precipitation endpoints."""

import hashlib
import json

from fastapi import APIRouter, HTTPException, Query
from fastapi import Path as PathParam
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse

from dependencies import logger, settings
from models.ecmwf_tp import (
    EcmwfTpPointValueResponse,
    ForecastListResponse,
    PeriodListResponse,
)
from routes.utils import create_tile_response
from services.ecmwf_tp_service import ecmwf_tp_service
from services.point_value_service import (
    CogNotFoundError,
    NoDataOrOutsideError,
    point_value_service,
)

router = APIRouter(prefix="/products/ecmwf", tags=["ECMWF Total Precipitation"])

_ZOOM_MIN = 3
_ZOOM_MAX = 7


@router.get(
    "/total-precipitation",
    status_code=status.HTTP_200_OK,
    summary="List ECMWF Forecast Runs",
    response_model=ForecastListResponse,
)
async def list_forecasts(request: Request):
    """List available ECMWF total precipitation forecast runs."""
    data = await ecmwf_tp_service.list_forecasts()
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
    "/total-precipitation/{forecast_ts}",
    status_code=status.HTTP_200_OK,
    summary="List Periods for a Forecast Run",
    response_model=PeriodListResponse,
)
async def list_periods(
    request: Request,
    forecast_ts: str = PathParam(
        ..., description="Forecast run timestamp (e.g. 20260328T1200Z)"
    ),
):
    """List all 6h precipitation accumulation periods available for a given ECMWF forecast run."""
    data = await ecmwf_tp_service.list_periods(forecast_ts)
    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Forecast '{forecast_ts}' not found",
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
    "/total-precipitation/{forecast_ts}/{period_ts}/{z}/{x}/{y}.webp",
    status_code=status.HTTP_200_OK,
    summary="Get ECMWF Precipitation Tile",
)
async def get_tile(
    request: Request,
    forecast_ts: str = PathParam(..., description="Forecast run timestamp"),
    period_ts: str = PathParam(
        ...,
        description="End timestamp of a 6h accumulation period (e.g. 20260329T1800Z)",
    ),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate"),
):
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Serve a single WebP tile for the given ECMWF period."""
    if z < _ZOOM_MIN or z > _ZOOM_MAX:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Zoom level {z} not available. Valid range: {_ZOOM_MIN}-{_ZOOM_MAX}",
        )

    etag = f'"{forecast_ts}-{period_ts}-{z}-{x}-{y}"'
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    tile_data = await ecmwf_tp_service.get_tile_data(forecast_ts, period_ts, z, x, y)
    if not tile_data:
        logger.warning(
            "ECMWF tile not found: %s/%s/%s/%s/%s", forecast_ts, period_ts, z, x, y
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Tile not found"
        )

    return create_tile_response(tile_data, etag, settings.cache_control_tile)


@router.get(
    "/total-precipitation/{forecast_ts}/{period_ts}/point",
    status_code=status.HTTP_200_OK,
    summary="Get ECMWF Point Value",
    response_model=EcmwfTpPointValueResponse,
)
async def get_point_value(
    forecast_ts: str = PathParam(..., description="Forecast run timestamp"),
    period_ts: str = PathParam(..., description="Period timestamp"),
    lat: float = Query(..., ge=-90.0, le=90.0, description="Latitude in EPSG:4326"),
    lon: float = Query(..., ge=-180.0, le=180.0, description="Longitude in EPSG:4326"),
):
    """Sample nearest precipitation value (mm) from the ECMWF COG at a lat/lon point."""
    try:
        sample = await point_value_service.sample_ecmwf_tp_point(
            forecast_ts, period_ts, lat, lon
        )
    except CogNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="cog_not_found"
        ) from exc
    except NoDataOrOutsideError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="nodata_or_outside"
        ) from exc

    return EcmwfTpPointValueResponse(
        forecast_ts=forecast_ts,
        period_ts=period_ts,
        lat=lat,
        lon=lon,
        value=sample.value,
        unit=sample.unit,
    )
