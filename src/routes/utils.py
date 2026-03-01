"""Shared utilities for route handlers."""

import io

from fastapi import Response
from PIL import Image


def _build_transparent_tile() -> bytes:
    img = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="WEBP", lossless=True)
    return buf.getvalue()


TRANSPARENT_TILE: bytes = _build_transparent_tile()


def make_transparent_tile_response(etag: str, cache_control: str) -> Response:
    """Return a 200 transparent WEBP tile (used as fallback for missing tiles)."""
    return create_tile_response(TRANSPARENT_TILE, etag, cache_control)


def create_tile_response(tile_data: bytes, etag: str, cache_control: str) -> Response:
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
