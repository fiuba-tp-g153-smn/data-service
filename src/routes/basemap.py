"""Base map tile proxy endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from fastapi import Path as PathParam
from fastapi import Request, Response, status

from dependencies import get_basemap_service, settings
from models.basemap import BasemapProvidersResponse
from routes.utils import (
    create_tile_response,
    etag_pair,
    make_transparent_png_response,
    not_modified,
)
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
    return await basemap_service.list_providers()


@router.get(
    "/{provider_id}/{z}/{x}/{y}.png",
    status_code=status.HTTP_200_OK,
    summary="Get Base Map Tile",
    response_description="PNG tile bytes (256×256)",
    description=(
        "Return a 256×256 PNG raster tile for the given base map provider "
        "and XYZ tile coordinates.\n\n"
        "Supports HTTP caching via `ETag` and `If-None-Match` "
        "(`304 Not Modified` when the client's ETag matches).\n\n"
        "Example: `GET /basemap/argenmap/4/5/9.png`."
    ),
    responses={
        200: {
            "content": {"image/png": {}},
            "description": "PNG tile with HTTP caching headers",
        },
        304: {"description": "Client's `If-None-Match` matched the current ETag"},
        503: {"description": "Base map service temporarily unavailable"},
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
    """Return a base map tile (PNG)."""
    if not basemap_service.validate_provider(provider_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown provider '{provider_id}'",
        )

    etag, miss_etag = etag_pair(f"{provider_id}-{z}-{x}-{y}")
    if_none_match = request.headers.get("if-none-match")
    if if_none_match == etag:
        return Response(status_code=status.HTTP_304_NOT_MODIFIED)

    try:
        tile_data = await basemap_service.get_tile_data(provider_id, z, x, y)
    except BasemapNotConfiguredError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    if not tile_data:
        if if_none_match == miss_etag:
            return not_modified(settings.basemap_cache_control_tile_miss)
        return make_transparent_png_response(
            miss_etag, settings.basemap_cache_control_tile_miss
        )

    return create_tile_response(
        tile_data, etag, settings.basemap_cache_control_tile, media_type="image/png"
    )
