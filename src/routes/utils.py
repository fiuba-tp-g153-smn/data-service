"""Shared utilities for route handlers."""

import hashlib
import io
import json
from typing import Any, Dict, Optional, Tuple

from fastapi import Response, status
from fastapi.responses import JSONResponse
from PIL import Image


def _build_transparent_tile() -> bytes:
    img = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="WEBP", lossless=True)
    return buf.getvalue()


def _build_transparent_png_tile() -> bytes:
    img = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


TRANSPARENT_TILE: bytes = _build_transparent_tile()
TRANSPARENT_PNG_TILE: bytes = _build_transparent_png_tile()


def etag_pair(identity: str) -> Tuple[str, str]:
    """`(hit, miss)` ETags for one tile identity.

    They must differ. A gap and the object that later fills it share a URL, so a
    single ETag would make the client's revalidation match its own cached gap
    and answer 304 forever — the tile would never arrive.
    """
    return f'"{identity}"', f'"{identity}-miss"'


def json_listing_response(
    payload: Dict[str, Any], if_none_match: Optional[str], cache_control: str
) -> Response:
    """A listing as a conditional GET: 304 when the caller already has it.

    Listings are polled far more often than they change — the frontend
    re-probes every product on a timer — so the steady state should be an empty
    304, not a full body. The ETag is the digest of the payload itself, which
    makes it exact: it changes when and only when the listing does, with no
    version counter to keep in step.
    """
    etag = (
        f'"{hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()}"'
    )
    if if_none_match and if_none_match == etag:
        return not_modified(cache_control)
    return JSONResponse(
        content=payload,
        headers={"Cache-Control": cache_control, "ETag": etag},
    )


def not_modified(cache_control: str) -> Response:
    """304 that restates the Cache-Control, so a gap keeps its short freshness."""
    return Response(
        status_code=status.HTTP_304_NOT_MODIFIED,
        headers={"Cache-Control": cache_control},
    )


def make_transparent_tile_response(etag: str, cache_control: str) -> Response:
    """Return a 200 transparent WEBP tile (used as fallback for missing tiles)."""
    return create_tile_response(TRANSPARENT_TILE, etag, cache_control)


def make_transparent_png_response(etag: str, cache_control: str) -> Response:
    """Return a 200 transparent PNG tile (miss fallback for PNG endpoints)."""
    return create_tile_response(
        TRANSPARENT_PNG_TILE, etag, cache_control, media_type="image/png"
    )


def create_tile_response(
    tile_data: bytes, etag: str, cache_control: str, media_type: str = "image/webp"
) -> Response:
    """Create a tile response with caching headers."""
    return Response(
        content=tile_data,
        media_type=media_type,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
            "Access-Control-Allow-Origin": "*",
        },
    )
