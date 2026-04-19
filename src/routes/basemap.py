"""Base map tile proxy endpoints."""

from fastapi import APIRouter, HTTPException
from fastapi import Path as PathParam
from fastapi import Request, Response, status

from dependencies import settings
from models.basemap import BasemapProvidersResponse
from routes.utils import create_tile_response
from services.basemap_service import basemap_service

router = APIRouter(prefix="/basemap", tags=["Basemap"])

_CACHE_CONTROL = "public, max-age=604800, immutable"


@router.get(
    "/providers",
    status_code=status.HTTP_200_OK,
    summary="List Base Map Providers",
    response_model=BasemapProvidersResponse,
)
async def list_providers():
    """List all available base map providers."""
    return basemap_service.list_providers()


@router.get(
    "/{provider_id}/{z}/{x}/{y}.png",
    status_code=status.HTTP_200_OK,
    summary="Get Base Map Tile",
)
async def get_tile(
    request: Request,
    provider_id: str = PathParam(..., description="Base map provider ID"),
    z: int = PathParam(..., description="Zoom level"),
    x: int = PathParam(..., description="Tile X coordinate"),
    y: int = PathParam(..., description="Tile Y coordinate"),
):
    """Serve a cached base map tile (PNG) with transparent proxy fallback."""
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

    tile_data = await basemap_service.get_tile_data(provider_id, z, x, y)
    if not tile_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tile not found",
        )

    return create_tile_response(tile_data, etag, _CACHE_CONTROL, media_type="image/png")
