"""Every endpoint that serves a placeholder in place of a missing tile must
give that placeholder its own ETag.

The gap and the tile that later fills it live at the same URL. With a single
ETag, a client that cached the gap revalidates, matches its own cached
placeholder and gets 304 back — forever. The real tile never arrives, even
once the data is there. That is not hypothetical for these routes: a basemap
gap means the upstream provider is down, and `S3Client.download_tile` reports
an infrastructure failure the same way it reports a genuine 404, so a
SeaweedFS blip paints placeholders into every client that asked during it.

One parametrization over the six affected handlers, so a new placeholder
endpoint that forgets `routes.utils.etag_pair` fails here.
"""

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Iterator
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

CYCLE = "20260808T0000Z"
INIT = "20260430_060000"
FXXX = "f003"

_WEBP = b"RIFF\x00\x00\x00\x00WEBPVP8 "
_GEOJSON = b'{"type":"FeatureCollection","features":[{"x":1}]}'


@dataclass(frozen=True)
class TileEndpoint:
    """One handler that serves a placeholder when its payload is missing."""

    name: str
    url: str
    hit_payload: bytes
    cache_control_attr: str
    expected_identity: str
    # Set the mocked service's return value for this endpoint's payload.
    install: Callable[..., object]

    def __str__(self) -> str:
        return self.name


@contextmanager
def _override_basemap(app) -> Iterator[Callable[[object], None]]:
    """Basemap resolves its service through `Depends`, not a module singleton."""
    from dependencies import get_basemap_service

    stub = AsyncMock()
    stub.validate_provider = lambda pid: True
    stub.get_tile_data = AsyncMock(return_value=None)
    app.dependency_overrides[get_basemap_service] = lambda: stub
    try:
        yield lambda value: setattr(
            stub, "get_tile_data", AsyncMock(return_value=value)
        )
    finally:
        app.dependency_overrides.pop(get_basemap_service, None)


def _patch_singleton(target: str, method: str):
    """The other routers import their service singleton at module load."""

    @contextmanager
    def _install(_app) -> Iterator[Callable[[object], None]]:
        with patch(target) as mock:
            setattr(mock, method, AsyncMock(return_value=None))
            yield lambda value: setattr(mock, method, AsyncMock(return_value=value))

    return _install


ENDPOINTS = [
    TileEndpoint(
        name="basemap-tile",
        url="/basemap/argenmap/4/5/9.png",
        hit_payload=b"\x89PNG\r\n\x1a\n",
        cache_control_attr="basemap_cache_control_tile_miss",
        expected_identity="argenmap-4-5-9",
        install=_override_basemap,
    ),
    TileEndpoint(
        name="radar-tile",
        url="/products/radar/RMA1/DBZH/elev0/20260808T120000Z/5/9/17.webp",
        hit_payload=_WEBP,
        cache_control_attr="radar_cache_control_tile_miss",
        expected_identity="RMA1-DBZH-elev0-20260808T120000Z-5-9-17",
        install=_patch_singleton("routes.radar.radar_service", "get_tile_data"),
    ),
    TileEndpoint(
        name="wrf-tile",
        url=f"/products/wrf/Colmax/{INIT}/{FXXX}/5/9/17.webp",
        hit_payload=_WEBP,
        cache_control_attr="wrf_cache_control_tile_miss",
        expected_identity=f"Colmax-{INIT}-{FXXX}-5-9-17",
        install=_patch_singleton("routes.wrf.wrf_service", "get_tile_data"),
    ),
    TileEndpoint(
        name="wrf-barbs",
        url=f"/products/wrf/Colmax/{INIT}/{FXXX}/barbs/4/5/9.json",
        hit_payload=_GEOJSON,
        cache_control_attr="wrf_cache_control_tile_miss",
        expected_identity=f"Colmax-{INIT}-{FXXX}-barbs-4-5-9",
        install=_patch_singleton("routes.wrf.wrf_service", "get_barb_tile"),
    ),
    TileEndpoint(
        name="gfs-tile",
        url=f"/products/gfs/500hpa/{CYCLE}/{FXXX}/5/9/17.webp",
        hit_payload=_WEBP,
        cache_control_attr="gfs_cache_control_tile_miss",
        expected_identity=f"500hpa-{CYCLE}-{FXXX}-5-9-17",
        install=_patch_singleton("routes.gfs.gfs_service", "get_tile_data"),
    ),
    TileEndpoint(
        name="gfs-barbs",
        url=f"/products/gfs/500hpa/{CYCLE}/{FXXX}/barbs/4/5/9.json",
        hit_payload=_GEOJSON,
        cache_control_attr="gfs_cache_control_tile_miss",
        expected_identity=f"500hpa-{CYCLE}-{FXXX}-barbs-4-5-9",
        install=_patch_singleton("routes.gfs.gfs_service", "get_barb_tile"),
    ),
]


@pytest.fixture(name="app")
def app_fixture(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    from main import app as fastapi_app  # pylint: disable=import-outside-toplevel

    return fastapi_app


@pytest.fixture(params=ENDPOINTS, ids=str, name="endpoint")
def endpoint_fixture(request, app):
    """Yield `(client, endpoint, set_payload)` for one placeholder endpoint."""
    endpoint: TileEndpoint = request.param
    with endpoint.install(app) as set_payload:
        yield TestClient(app), endpoint, set_payload


def test_miss_etag_differs_from_hit_etag(endpoint):
    client, spec, set_payload = endpoint

    set_payload(None)
    miss_etag = client.get(spec.url).headers["etag"]

    set_payload(spec.hit_payload)
    hit_etag = client.get(spec.url).headers["etag"]

    assert hit_etag == f'"{spec.expected_identity}"'
    assert miss_etag == f'"{spec.expected_identity}-miss"'
    assert miss_etag != hit_etag


def test_revalidating_a_cached_miss_yields_the_recovered_payload(endpoint):
    """The regression the whole hit/miss ETag pair exists for.

    Data is missing → the client caches a placeholder. The data lands. The
    client revalidates with the miss ETag and must get the real payload, not a
    304 pointing back at its own cached gap.
    """
    client, spec, set_payload = endpoint

    set_payload(None)
    miss = client.get(spec.url)
    assert miss.status_code == 200

    set_payload(spec.hit_payload)
    revalidated = client.get(spec.url, headers={"If-None-Match": miss.headers["etag"]})

    assert revalidated.status_code == 200
    assert revalidated.content == spec.hit_payload


def test_revalidating_a_still_missing_tile_304s_with_the_miss_cache_control(endpoint):
    """Still missing: 304 is right, but it must restate the short miss TTL so
    the gap keeps its short freshness instead of inheriting a heuristic."""
    client, spec, set_payload = endpoint
    set_payload(None)

    from dependencies import settings  # pylint: disable=import-outside-toplevel

    miss = client.get(spec.url)
    revalidated = client.get(spec.url, headers={"If-None-Match": miss.headers["etag"]})

    assert revalidated.status_code == 304
    assert revalidated.headers["cache-control"] == getattr(
        settings, spec.cache_control_attr
    )


def test_miss_is_not_served_with_an_immutable_cache_control(endpoint):
    """`immutable` tells the browser not to revalidate at all, which is exactly
    what pins a placeholder in place while the real data is unreachable."""
    client, spec, set_payload = endpoint
    set_payload(None)

    response = client.get(spec.url)

    assert response.status_code == 200
    assert "immutable" not in response.headers["cache-control"]


def test_revalidating_a_cached_hit_still_304s(endpoint):
    """The hit path's 304 must keep working — the fix must not cost the
    bandwidth saving on payloads that are genuinely unchanged."""
    client, spec, set_payload = endpoint
    set_payload(spec.hit_payload)

    hit = client.get(spec.url)
    revalidated = client.get(spec.url, headers={"If-None-Match": hit.headers["etag"]})

    assert revalidated.status_code == 304
