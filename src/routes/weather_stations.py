"""HTTP routes for the weather-stations subsystem."""

import hmac
import logging
from typing import Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query
from fastapi import Path as PathParam
from fastapi import Response, status

from clients.weather_stations_keystore import (
    SecretAlreadyInUseError,
    WeatherStationsKeystore,
)
from dependencies import (
    get_weather_stations_keystore,
    get_weather_stations_service,
    settings,
)
from models.weather_stations import (
    AdminKeyAddCustomRequest,
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
# Admin endpoints live on a sibling router with a different tag so Swagger UI
# can render them as a distinct section. Both routers share the same prefix
# and dependencies but are scoped to disjoint tags.
admin_router = APIRouter(
    prefix="/weather-stations/admin", tags=["Weather Stations · Admin"]
)

_API_KEY_HEADER_DOC = "API key for read endpoints."
_ADMIN_PASSWORD_HEADER_DOC = "Master password for admin endpoints."


# ---------------------------------------------------------------------- auth deps


async def require_api_key(
    x_api_key: Optional[str] = Header(
        default=None,
        description=_API_KEY_HEADER_DOC,
        examples=["kZ8sJ3w_2x...token_urlsafe(32)"],
    ),
    keystore: WeatherStationsKeystore = Depends(get_weather_stations_keystore),
) -> None:
    """Gate read endpoints with the API-key keystore.

    When `weather_stations_api_key_auth_enabled=false` (local-dev only) the
    check is skipped — every request is allowed through.
    """
    if not settings.weather_stations_api_key_auth_enabled:
        return
    if not x_api_key:
        logger.info("weather-stations 401: missing X-API-Key header")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header",
        )
    if not await keystore.is_valid(x_api_key):
        # Last 4 chars only — enough to disambiguate which key was rejected,
        # not enough to leak the secret to logs.
        logger.info(
            "weather-stations 401: invalid X-API-Key (suffix=...%s)",
            x_api_key[-4:] if len(x_api_key) >= 4 else "***",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header",
        )


async def require_admin_password(
    x_admin_password: Optional[str] = Header(
        default=None,
        description=_ADMIN_PASSWORD_HEADER_DOC,
        examples=["your-master-password"],
    ),
) -> None:
    """Gate admin endpoints with the master password (constant-time compare)."""
    expected = settings.weather_stations_admin_password
    if not expected:
        # Defensive: validator should already enforce this, but a misconfigured
        # deployment must NOT silently accept any header value.
        logger.error(
            "weather-stations admin endpoint reached with no admin password "
            "configured — rejecting with 503"
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin endpoints unavailable (no admin password configured)",
        )
    if not x_admin_password:
        logger.info("weather-stations admin 401: missing X-Admin-Password header")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-Admin-Password header",
        )
    if not hmac.compare_digest(x_admin_password, expected):
        logger.warning("weather-stations admin 401: X-Admin-Password mismatch")
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
    description=(
        "Return the most recent observation snapshot scraped from the SMN "
        "`/weather/station` endpoint. Refreshed every 5 minutes.\n\n"
        "Each station carries its own `observed_at`; values may be older "
        "than `scraped_at` because SMN stations report hourly/3-hourly.\n\n"
        '**Example:** `curl -H "X-API-Key: $KEY" '
        "http://localhost:8080/weather-stations/latest`"
    ),
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
    description=(
        "List the hour-bucketed `tileset_id`s currently available in S3 "
        "(retention window is `weather_stations_s3_object_ttl_days`, "
        "default 2 days). Each entry maps to a snapshot you can fetch via "
        "`GET /weather-stations/{tileset_id}`.\n\n"
        '**Example:** `curl -H "X-API-Key: $KEY" '
        "http://localhost:8080/weather-stations/tilesets`"
    ),
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
    description=(
        "Return the canonical SMN EMA station registry (id, name, province, "
        "decimal-degree coordinates, altitude, OACI code).\n\n"
        "Sourced from "
        "`http://ssl.smn.gob.ar/dpd/zipopendata.php?dato=estaciones` and "
        "refreshed by the scraper only when the upstream payload's hash "
        "changes. Cache aggressively client-side.\n\n"
        '**Example:** `curl -H "X-API-Key: $KEY" '
        "http://localhost:8080/weather-stations/stations`"
    ),
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
    description=(
        "Return the latest snapshot whose `scraped_at` falls in "
        "`[tileset_id - N hours, tileset_id]`. `N` defaults to 0 (exact "
        "hour match required) and is capped at 48.\n\n"
        '**Example:** `curl -H "X-API-Key: $KEY" '
        "'http://localhost:8080/weather-stations/20260517T1400Z?N=6'`"
    ),
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
        description="Hour-bucket id in `YYYYMMDDTHH00Z` UTC format.",
        examples=["20260517T1400Z"],
    ),
    n: float = Query(
        0,
        ge=0,
        le=48,
        alias="N",
        description=(
            "Tolerance window in hours; the snapshot returned has "
            "`scraped_at` in `[tileset_id - N hours, tileset_id]`."
        ),
        examples=[0, 6],
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


@admin_router.post(
    "/keys",
    status_code=status.HTTP_201_CREATED,
    summary="Create a new API key",
    response_model=AdminKeyCreateResponse,
    dependencies=[Depends(require_admin_password)],
    description="Mint a new API key. The plaintext secret is returned once.",
    responses={
        201: {"description": "Key created; secret returned once"},
        401: {"description": "Missing or invalid admin password"},
    },
)
async def create_api_key(
    body: AdminKeyCreateRequest = Body(...),
    keystore: WeatherStationsKeystore = Depends(get_weather_stations_keystore),
) -> AdminKeyCreateResponse:
    """Mint a new API key. Plaintext secret is returned exactly once."""
    created = await keystore.create(body.label)
    logger.info(
        "weather-stations admin: API key created id=%s label=%r",
        created.key_id,
        created.label,
    )
    return make_admin_create_response(
        key_id=created.key_id,
        label=created.label,
        secret=created.secret,
        created_at_epoch=created.created_at,
    )


@admin_router.post(
    "/keys/add-custom",
    status_code=status.HTTP_201_CREATED,
    summary="Add an API key with a caller-provided secret",
    response_model=AdminKeyCreateResponse,
    dependencies=[Depends(require_admin_password)],
    description="Register a caller-supplied plaintext secret as a valid API key.",
    responses={
        201: {"description": "Key added; secret echoed back"},
        401: {"description": "Missing or invalid admin password"},
        409: {"description": "Secret already in use"},
    },
)
async def add_custom_api_key(
    body: AdminKeyAddCustomRequest = Body(...),
    keystore: WeatherStationsKeystore = Depends(get_weather_stations_keystore),
) -> AdminKeyCreateResponse:
    """Register a caller-provided secret as a valid API key."""
    try:
        created = await keystore.add_custom(body.label, body.secret)
    except SecretAlreadyInUseError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Secret already in use",
        ) from exc
    logger.info(
        "weather-stations admin: API key added (custom) id=%s label=%r",
        created.key_id,
        created.label,
    )
    return make_admin_create_response(
        key_id=created.key_id,
        label=created.label,
        secret=created.secret,
        created_at_epoch=created.created_at,
    )


@admin_router.get(
    "/keys",
    status_code=status.HTTP_200_OK,
    summary="List API keys",
    response_model=AdminKeyListResponse,
    dependencies=[Depends(require_admin_password)],
    description="List every active API key (without secrets).",
    responses={401: {"description": "Missing or invalid admin password"}},
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


@admin_router.delete(
    "/keys/{key_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke an API key",
    dependencies=[Depends(require_admin_password)],
    description="Revoke an API key by id. Takes effect immediately.",
    responses={
        204: {"description": "Key revoked"},
        401: {"description": "Missing or invalid admin password"},
        404: {"description": "Unknown key id"},
    },
)
async def revoke_api_key(
    key_id: str = PathParam(
        ...,
        description="Id returned when the key was created.",
        examples=["1a2b3c4d5e6f7890"],
    ),
    keystore: WeatherStationsKeystore = Depends(get_weather_stations_keystore),
) -> Response:
    """Revoke an API key by id."""
    removed = await keystore.revoke(key_id)
    if not removed:
        logger.info("weather-stations admin: revoke skipped, unknown key id=%s", key_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown key_id {key_id!r}",
        )
    logger.info("weather-stations admin: API key revoked id=%s", key_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
