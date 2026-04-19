"""Base map tile proxy endpoints."""

from fastapi import APIRouter, HTTPException
from fastapi import Path as PathParam
from fastapi import Request, Response, status

from dependencies import settings
from models.basemap import BasemapProvidersResponse
from routes.utils import create_tile_response
from services.basemap_service import BasemapNotConfiguredError, basemap_service

router = APIRouter(prefix="/basemap", tags=["Basemap"])


@router.get(
    "/providers",
    status_code=status.HTTP_200_OK,
    summary="List Base Map Providers",
    response_model=BasemapProvidersResponse,
)
async def list_providers() -> BasemapProvidersResponse:
    """List all available base map providers."""
    return basemap_service.list_providers()


@router.get(
    "/{provider_id}/{z}/{x}/{y}.png",
    status_code=status.HTTP_200_OK,
    summary="Get Base Map Tile",
    responses={
        200: {"content": {"image/png": {}}, "description": "PNG tile"},
        304: {"description": "Not Modified (ETag match)"},
        400: {"description": "Zoom level not available for provider"},
        404: {"description": "Provider or tile not found"},
        503: {"description": "Basemap service not configured"},
    },
)
async def get_tile(
    request: Request,
    provider_id: str = PathParam(..., description="Base map provider ID"),
    z: int = PathParam(..., ge=0, le=22, description="Zoom level"),
    x: int = PathParam(..., ge=0, description="Tile X coordinate"),
    y: int = PathParam(..., ge=0, description="Tile Y coordinate"),
) -> Response:
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
