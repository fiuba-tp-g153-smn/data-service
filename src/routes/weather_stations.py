"""HTTP routes for the weather-stations subsystem."""

import hmac
import logging
from typing import Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query
from fastapi import Path as PathParam
from fastapi import Response, status

from clients.weather_stations_keystore import WeatherStationsKeystore
from dependencies import (
    get_weather_stations_keystore,
    get_weather_stations_service,
    settings,
)
from models.weather_stations import (
    AdminKeyCreateRequest,
    AdminKeyCreateResponse,
    AdminKeyListResponse,
    StationsRegistryResponse,
    TilesetsResponse,
    WeatherStationsSnapshot,
    make_admin_create_response,
    make_admin_list_entry,
)
from services.weather_stations_service import (
    TilesetIdFormatError,
    WeatherStationsNotConfiguredError,
    WeatherStationsService,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/weather-stations", tags=["Weather Stations"])


# ---------------------------------------------------------------------- auth deps


async def require_api_key(
    x_api_key: Optional[str] = Header(
        default=None,
        description="API key minted via the admin endpoints.",
        examples=["abc123…"],
    ),
    keystore: WeatherStationsKeystore = Depends(get_weather_stations_keystore),
) -> None:
    """Gate read endpoints with the API-key keystore.

    When `weather_stations_api_key_auth_enabled=false` (local-dev only) the
    check is skipped — every request is allowed through.
    """
    if not settings.weather_stations_api_key_auth_enabled:
        return
    if not x_api_key or not await keystore.is_valid(x_api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header",
        )


async def require_admin_password(
    x_admin_password: Optional[str] = Header(
        default=None,
        description="Master password configured via WEATHER_STATIONS_ADMIN_PASSWORD.",
    ),
) -> None:
    """Gate admin endpoints with the master password (constant-time compare)."""
    expected = settings.weather_stations_admin_password
    if not expected:
        # Defensive: validator should already enforce this, but a misconfigured
        # deployment must NOT silently accept any header value.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin endpoints unavailable (no admin password configured)",
        )
    if not x_admin_password or not hmac.compare_digest(x_admin_password, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-Admin-Password header",
        )


# ---------------------------------------------------------------------- helpers


def _response_with_cache_control() -> dict:
    return {"Cache-Control": settings.weather_stations_cache_control_response}


def _raise_not_configured(exc: Exception) -> None:
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=str(exc),
    ) from exc


# ----------------------------------------------------------------- read routes


@router.get(
    "/latest",
    status_code=status.HTTP_200_OK,
    summary="Latest weather-stations snapshot",
    response_model=WeatherStationsSnapshot,
    dependencies=[Depends(require_api_key)],
    responses={
        401: {"description": "Missing or invalid X-API-Key"},
        503: {"description": "Snapshot not yet available (cold boot or S3 down)"},
    },
)
async def get_latest(
    response: Response,
    service: WeatherStationsService = Depends(get_weather_stations_service),
) -> WeatherStationsSnapshot:
    """Most recent scrape's snapshot."""
    try:
        payload = await service.get_latest_snapshot()
    except WeatherStationsNotConfiguredError as exc:
        _raise_not_configured(exc)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Latest snapshot not yet available; check back after first scrape",
        )
    response.headers.update(_response_with_cache_control())
    return WeatherStationsSnapshot.model_validate(payload)


@router.get(
    "/tilesets",
    status_code=status.HTTP_200_OK,
    summary="Available time buckets",
    response_model=TilesetsResponse,
    dependencies=[Depends(require_api_key)],
    responses={401: {"description": "Missing or invalid X-API-Key"}},
)
async def list_tilesets(
    response: Response,
    service: WeatherStationsService = Depends(get_weather_stations_service),
) -> TilesetsResponse:
    """Hour-bucketed list of available snapshots in the retention window."""
    try:
        entries = await service.list_tilesets()
    except WeatherStationsNotConfiguredError as exc:
        _raise_not_configured(exc)
    response.headers.update(_response_with_cache_control())
    return TilesetsResponse.model_validate({"tilesets": entries})


@router.get(
    "/stations",
    status_code=status.HTTP_200_OK,
    summary="Station registry (metadata)",
    response_model=StationsRegistryResponse,
    dependencies=[Depends(require_api_key)],
    responses={
        401: {"description": "Missing or invalid X-API-Key"},
        503: {"description": "Registry not yet populated"},
    },
)
async def get_registry(
    response: Response,
    service: WeatherStationsService = Depends(get_weather_stations_service),
) -> StationsRegistryResponse:
    """Canonical SMN EMA registry — refreshed by the scraper when upstream changes."""
    try:
        payload = await service.get_stations_registry()
    except WeatherStationsNotConfiguredError as exc:
        _raise_not_configured(exc)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Stations registry not yet available; check back after first scrape",
        )
    response.headers.update(_response_with_cache_control())
    return StationsRegistryResponse.model_validate(payload)


@router.get(
    "/{tileset_id}",
    status_code=status.HTTP_200_OK,
    summary="Snapshot for a specific time bucket",
    response_model=WeatherStationsSnapshot,
    dependencies=[Depends(require_api_key)],
    responses={
        400: {"description": "Malformed tilesetId"},
        401: {"description": "Missing or invalid X-API-Key"},
        404: {"description": "No snapshot found within the requested window"},
    },
)
async def get_for_tileset(
    response: Response,
    tileset_id: str = PathParam(
        ...,
        description="Hour-bucket id, e.g. `20260517T1400Z`",
        examples=["20260517T1400Z"],
    ),
    n: float = Query(
        0,
        ge=0,
        le=48,
        alias="N",
        description="Tolerance window in hours; the snapshot returned has "
        "scraped_at in [tilesetId - N hours, tilesetId].",
    ),
    service: WeatherStationsService = Depends(get_weather_stations_service),
) -> WeatherStationsSnapshot:
    """Pick the latest snapshot whose scrape time falls within the N-hour window."""
    try:
        payload = await service.get_snapshot_for_tileset(tileset_id, n)
    except TilesetIdFormatError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except WeatherStationsNotConfiguredError as exc:
        _raise_not_configured(exc)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No snapshot available for tilesetId={tileset_id} "
                f"with N={n}h tolerance"
            ),
        )
    response.headers.update(_response_with_cache_control())
    return WeatherStationsSnapshot.model_validate(payload)


# ---------------------------------------------------------------- admin routes


@router.post(
    "/admin/keys",
    status_code=status.HTTP_201_CREATED,
    summary="Create a new API key",
    response_model=AdminKeyCreateResponse,
    dependencies=[Depends(require_admin_password)],
    responses={401: {"description": "Missing or invalid X-Admin-Password"}},
)
async def create_api_key(
    body: AdminKeyCreateRequest = Body(...),
    keystore: WeatherStationsKeystore = Depends(get_weather_stations_keystore),
) -> AdminKeyCreateResponse:
    """Mint a new API key. Plaintext secret is returned exactly once."""
    created = await keystore.create(body.label)
    return make_admin_create_response(
        key_id=created.key_id,
        label=created.label,
        secret=created.secret,
        created_at_epoch=created.created_at,
    )


@router.get(
    "/admin/keys",
    status_code=status.HTTP_200_OK,
    summary="List API keys",
    response_model=AdminKeyListResponse,
    dependencies=[Depends(require_admin_password)],
    responses={401: {"description": "Missing or invalid X-Admin-Password"}},
)
async def list_api_keys(
    keystore: WeatherStationsKeystore = Depends(get_weather_stations_keystore),
) -> AdminKeyListResponse:
    """List every API key (without secrets)."""
    records = await keystore.list_all()
    return AdminKeyListResponse(
        keys=[
            make_admin_list_entry(
                key_id=r.key_id,
                label=r.label,
                created_at_epoch=r.created_at,
                last_used_at_epoch=r.last_used_at,
            )
            for r in records
        ]
    )


@router.delete(
    "/admin/keys/{key_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke an API key",
    dependencies=[Depends(require_admin_password)],
    responses={
        204: {"description": "Key revoked"},
        401: {"description": "Missing or invalid X-Admin-Password"},
        404: {"description": "Unknown key_id"},
    },
)
async def revoke_api_key(
    key_id: str = PathParam(..., description="ID returned by POST /admin/keys"),
    keystore: WeatherStationsKeystore = Depends(get_weather_stations_keystore),
) -> Response:
    """Revoke an API key by id."""
    removed = await keystore.revoke(key_id)
    if not removed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown key_id {key_id!r}",
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
