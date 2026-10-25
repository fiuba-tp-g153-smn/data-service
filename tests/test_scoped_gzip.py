"""PathScopedGZipMiddleware compresses only requests under its prefixes."""

from fastapi.testclient import TestClient
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from scoped_gzip import PathScopedGZipMiddleware

_BIG = [{"domain": "satellite", "downloaded": i} for i in range(200)]  # ~6 KB
_GZIP = {"Accept-Encoding": "gzip"}


def _client() -> TestClient:
    async def big(_request):
        return JSONResponse(_BIG)

    async def small(_request):
        return JSONResponse({"ok": True})

    app = Starlette(
        routes=[
            Route("/metrics/big", big),
            Route("/metrics/small", small),
            Route("/other/big", big),
        ]
    )
    app.add_middleware(PathScopedGZipMiddleware, prefixes=("/metrics/",))
    return TestClient(app)


def test_prefixed_path_is_gzipped_and_decodes_intact():
    response = _client().get("/metrics/big", headers=_GZIP)

    assert response.headers.get("content-encoding") == "gzip"
    assert response.num_bytes_downloaded < len(response.content)
    assert response.json() == _BIG


def test_other_paths_are_left_uncompressed():
    response = _client().get("/other/big", headers=_GZIP)

    assert "content-encoding" not in response.headers
    assert response.json() == _BIG


def test_small_responses_are_not_compressed():
    response = _client().get("/metrics/small", headers=_GZIP)

    assert "content-encoding" not in response.headers


def test_clients_that_do_not_accept_gzip_get_plain_json():
    response = _client().get("/metrics/big", headers={"Accept-Encoding": "identity"})

    assert "content-encoding" not in response.headers
    assert response.json() == _BIG
