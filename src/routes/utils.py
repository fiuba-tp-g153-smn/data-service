"""Shared utilities for route handlers."""

from fastapi import Response


def create_tile_response(
    tile_data: bytes, etag: str, cache_control: str
) -> Response:
    """Create a standard WebP tile response with caching headers."""
    return Response(
        content=tile_data,
        media_type="image/webp",
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
            "Access-Control-Allow-Origin": "*",
        },
    )
