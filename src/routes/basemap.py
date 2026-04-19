"""Base map tile proxy endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from fastapi import Path as PathParam
from fastapi import Request, Response, status

from dependencies import get_basemap_service, settings
from models.basemap import BasemapProvidersResponse
from routes.utils import create_tile_response
from services.basemap_service import BasemapNotConfiguredError, BasemapService

router = APIRouter(prefix="/basemap", tags=["Basemap"])


@router.get(
    "/providers",
    status_code=status.HTTP_200_OK,
    summary="List Base Map Providers",
    response_model=BasemapProvidersResponse,
)
async def list_providers(
    basemap_service: BasemapService = Depends(get_basemap_service),
) -> BasemapProvidersResponse:
    """List all base map providers enabled in settings.json.

    Use the returned `id` as `{provider_id}` in `/basemap/{provider_id}/{z}/{x}/{y}.png`.
    """
    return basemap_service.list_providers()


@router.get(
    "/{provider_id}/{z}/{x}/{y}.png",
    status_code=status.HTTP_200_OK,
    summary="Get Base Map Tile",
    response_description="PNG tile bytes (256x256)",
    description=(
        "Serve a base map raster tile (XYZ scheme) from a 3-tier cache:\n\n"
        "1. **Redis** — hot cache with TTL.\n"
        "2. **S3** — cold backup populated by the weekly scraper.\n"
        "3. **External provider** — online proxy fallback. Disabled when "
        "`basemap_online_fallback_enabled=false` in settings.json, in which "
        "case misses return 404 (fully-offline serving).\n\n"
        "`ETag` is returned for every tile; clients may send `If-None-Match` "
        "to get `304 Not Modified`.\n\n"
        "Example: `GET /basemap/argenmap/4/5/9.png` → PNG tile for zoom 4, "
        "x=5, y=9 from the Argenmap (IGN) provider."
    ),
    responses={
        200: {
            "content": {"image/png": {}},
            "description": "PNG tile (256×256) with Cache-Control + ETag headers",
        },
        304: {"description": "Not Modified — client's ETag matched"},
        400: {"description": "Zoom level out of range for this provider"},
        404: {"description": "Unknown provider, or tile not cached (offline mode)"},
        503: {"description": "Basemap subsystem not configured at startup"},
    },
)
async def get_tile(
    request: Request,
    provider_id: str = PathParam(
        ...,
        description="Base map provider ID (see `/basemap/providers`)",
        examples=["argenmap"],
    ),
    z: int = PathParam(
        ...,
        ge=0,
        le=22,
        description="Zoom level (0 = whole world, ~22 = sub-meter)",
        examples=[4],
    ),
    x: int = PathParam(
        ...,
        ge=0,
        description="Tile X coordinate (column, left-to-right)",
        examples=[5],
    ),
    y: int = PathParam(
        ...,
        ge=0,
        description="Tile Y coordinate (row, top-to-bottom in XYZ scheme)",
        examples=[9],
    ),
    basemap_service: BasemapService = Depends(get_basemap_service),
) -> Response:
    """Serve a cached base map tile (PNG)."""
    if not basemap_service.validate_provider(provider_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown provider '{provider_id}'",
        )

    if not basemap_service.validate_zoom(provider_id, z):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Zoom level {z} not available for provider '{provider_id}'",
        )

    etag = f'"{provider_id}-{z}-{x}-{y}"'
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    try:
        tile_data = await basemap_service.get_tile_data(provider_id, z, x, y)
    except BasemapNotConfiguredError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    if not tile_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tile not found",
        )

    return create_tile_response(
        tile_data, etag, settings.cache_control_tile, media_type="image/png"
    )
