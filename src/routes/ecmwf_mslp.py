"""ECMWF mean sea level pressure endpoints."""

import hashlib
import json

from fastapi import APIRouter, HTTPException, Query
from fastapi import Path as PathParam
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse

from dependencies import logger, settings
from models.ecmwf_mslp import (
    EcmwfMslpPointValueResponse,
    MslpForecastListResponse,
    MslpTimestampListResponse,
)
from services.ecmwf_mslp_service import ecmwf_mslp_service
from services.point_value_service import (
    CogNotFoundError,
    NoDataOrOutsideError,
    point_value_service,
)

router = APIRouter(prefix="/products/ecmwf", tags=["ECMWF Mean Sea Level Pressure"])


@router.get(
    "/mean-sea-level-pressure",
    status_code=status.HTTP_200_OK,
    summary="List ECMWF MSLP Forecast Runs",
    response_model=MslpForecastListResponse,
)
async def list_forecasts(request: Request):
    """List available ECMWF mean sea level pressure forecast runs."""
    data = await ecmwf_mslp_service.list_forecasts()
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
    "/mean-sea-level-pressure/{forecast_ts}",
    status_code=status.HTTP_200_OK,
    summary="List Timestamps for a MSLP Forecast Run",
    response_model=MslpTimestampListResponse,
)
async def list_timestamps(
    request: Request,
    forecast_ts: str = PathParam(
        ..., description="Forecast run timestamp (e.g. 20260413T1200Z)"
    ),
):
    """List all MSLP timestamps available for a given ECMWF forecast run."""
    data = await ecmwf_mslp_service.list_timestamps(forecast_ts)
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
    "/mean-sea-level-pressure/{forecast_ts}/{timestamp_ts}/point",
    status_code=status.HTTP_200_OK,
    summary="Get ECMWF MSLP Point Value",
    response_model=EcmwfMslpPointValueResponse,
)
async def get_point_value(
    forecast_ts: str = PathParam(..., description="Forecast run timestamp"),
    timestamp_ts: str = PathParam(..., description="MSLP timestamp"),
    lat: float = Query(..., ge=-90.0, le=90.0, description="Latitude in EPSG:4326"),
    lon: float = Query(..., ge=-180.0, le=180.0, description="Longitude in EPSG:4326"),
):
    """Sample nearest pressure value (hPa) from the ECMWF MSLP COG at a lat/lon point."""
    try:
        sample = await point_value_service.sample_ecmwf_mslp_point(
            forecast_ts, timestamp_ts, lat, lon
        )
    except CogNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="cog_not_found"
        ) from exc
    except NoDataOrOutsideError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="nodata_or_outside"
        ) from exc

    return EcmwfMslpPointValueResponse(
        forecast_ts=forecast_ts,
        timestamp_ts=timestamp_ts,
        lat=lat,
        lon=lon,
        value=sample.value,
        unit=sample.unit,
    )


@router.get(
    "/mean-sea-level-pressure/{forecast_ts}/{timestamp_ts}.json",
    status_code=status.HTTP_200_OK,
    summary="Get ECMWF MSLP Isobars GeoJSON",
)
async def get_isobars_geojson(
    request: Request,
    forecast_ts: str = PathParam(..., description="Forecast run timestamp"),
    timestamp_ts: str = PathParam(..., description="MSLP timestamp"),
):
    """Serve the simplified isobars GeoJSON for the given MSLP timestamp."""
    etag = f'"{forecast_ts}-{timestamp_ts}"'
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    data = await ecmwf_mslp_service.get_geojson(forecast_ts, timestamp_ts)
    if not data:
        logger.warning("ECMWF-MSLP GeoJSON not found: %s/%s", forecast_ts, timestamp_ts)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="GeoJSON not found"
        )

    return Response(
        content=data,
        media_type="application/geo+json",
        headers={
            "Cache-Control": settings.cache_control_tile,
            "ETag": etag,
        },
    )
