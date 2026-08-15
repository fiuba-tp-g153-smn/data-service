"""Endpoint tests for the GFS router.

The routing itself is the fragile part here: five of the six routes live under
`/{product_id}/{cycle}/{fxxx}/...` and are told apart only by their suffix and
by the literal `barbs`/`point` segments. These tests pin that resolution.
"""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

CYCLE = "20260808T0000Z"
FXXX = "f003"
BASE = f"/products/gfs/500hpa/{CYCLE}/{FXXX}"


@pytest.fixture
def app_client(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    from main import app  # pylint: disable=import-outside-toplevel

    return TestClient(app)


@pytest.fixture
def service():
    """Patch the singleton the router imported, not the module attribute."""
    with patch("routes.gfs.gfs_service") as mock:
        mock.list_cycles = AsyncMock(return_value=None)
        mock.list_steps = AsyncMock(return_value=None)
        mock.get_tile_data = AsyncMock(return_value=None)
        mock.get_geojson = AsyncMock(return_value=None)
        mock.get_barb_tile = AsyncMock(return_value=None)
        yield mock


class TestRouteResolution:
    """Each URL must reach the handler it looks like it should."""

    def test_point_is_not_swallowed_by_the_layer_route(self, app_client, service):
        with patch("routes.gfs.point_value_service") as pvs:
            pvs.sample_gfs_point = AsyncMock(side_effect=RuntimeError("reached"))
            with pytest.raises(RuntimeError, match="reached"):
                app_client.get(f"{BASE}/point?lat=-34&lon=-64")
        service.get_geojson.assert_not_called()

    def test_barbs_are_not_swallowed_by_the_tile_route(self, app_client, service):
        app_client.get(f"{BASE}/barbs/4/5/9.json")
        service.get_barb_tile.assert_awaited_once()
        service.get_tile_data.assert_not_called()

    def test_layer_json_reaches_the_overlay_handler(self, app_client, service):
        app_client.get(f"{BASE}/heights.json")
        service.get_geojson.assert_awaited_once()
        args = service.get_geojson.await_args.args
        assert args[3] == "heights"

    def test_webp_reaches_the_tile_handler(self, app_client, service):
        app_client.get(f"{BASE}/5/9/17.webp")
        service.get_tile_data.assert_awaited_once()
        service.get_geojson.assert_not_called()


class TestListings:
    def test_unknown_product_is_404(self, app_client, service):
        response = app_client.get("/products/gfs/850hpa")
        assert response.status_code == 404

    def test_unknown_cycle_is_404(self, app_client, service):
        response = app_client.get(f"/products/gfs/500hpa/{CYCLE}")
        assert response.status_code == 404

    def test_cycle_listing_is_served_with_an_etag(self, app_client, service):
        from models.base import BoundingBox, ZoomLevels  # pylint: disable=C0415
        from models.gfs import GfsCycleListResponse  # pylint: disable=C0415

        service.list_cycles = AsyncMock(
            return_value=GfsCycleListResponse(
                product_id="500hpa",
                cycles=[],
                layers=["heights"],
                tile_url_pattern="/x/{z}/{x}/{y}.webp",
                zoom_levels=ZoomLevels(min=3, max=7),
                bounding_box=BoundingBox(minx=-110, miny=-60, maxx=-30, maxy=-15),
            )
        )
        response = app_client.get("/products/gfs/500hpa")
        assert response.status_code == 200
        assert response.headers.get("ETag")

    def test_step_listing_is_served_with_an_etag(self, app_client, service):
        from models.base import BoundingBox, ZoomLevels  # pylint: disable=C0415
        from models.gfs import (  # pylint: disable=C0415
            GfsStepInfo,
            GfsStepListResponse,
        )

        service.list_steps = AsyncMock(
            return_value=GfsStepListResponse(
                product_id="500hpa",
                cycle=CYCLE,
                steps=[
                    GfsStepInfo(
                        fxxx=FXXX, valid_ts="20260808T0300Z", layers=["heights"]
                    )
                ],
                tile_url_pattern="/x/{z}/{x}/{y}.webp",
                barb_tile_url_pattern="/x/barbs/{z}/{x}/{y}.json",
                barb_zoom_levels=[2, 4, 6, 8],
                zoom_levels=ZoomLevels(min=3, max=7),
                bounding_box=BoundingBox(minx=-110, miny=-60, maxx=-30, maxy=-15),
            )
        )
        response = app_client.get(f"/products/gfs/500hpa/{CYCLE}")
        assert response.status_code == 200
        assert response.headers.get("ETag")
        body = response.json()
        assert body["steps"][0]["layers"] == ["heights"]
        assert body["barb_zoom_levels"] == [2, 4, 6, 8]

        cached = app_client.get(
            f"/products/gfs/500hpa/{CYCLE}",
            headers={"If-None-Match": response.headers["ETag"]},
        )
        assert cached.status_code == 304

    def test_matching_etag_returns_304(self, app_client, service):
        from models.base import BoundingBox, ZoomLevels  # pylint: disable=C0415
        from models.gfs import GfsCycleListResponse  # pylint: disable=C0415

        service.list_cycles = AsyncMock(
            return_value=GfsCycleListResponse(
                product_id="500hpa",
                cycles=[],
                layers=["heights"],
                tile_url_pattern=None,
                zoom_levels=ZoomLevels(min=3, max=7),
                bounding_box=BoundingBox(minx=-110, miny=-60, maxx=-30, maxy=-15),
            )
        )
        first = app_client.get("/products/gfs/500hpa")
        second = app_client.get(
            "/products/gfs/500hpa", headers={"If-None-Match": first.headers["ETag"]}
        )
        assert second.status_code == 304


class TestTiles:
    def test_missing_tile_is_transparent_not_404(self, app_client, service):
        """gdal2tiles only emits tiles the model covers; gaps are normal."""
        response = app_client.get(f"{BASE}/5/9/17.webp")
        assert response.status_code == 200
        assert response.headers["content-type"] == "image/webp"

    @pytest.mark.parametrize("zoom", [2, 8])
    def test_zoom_outside_the_pyramid_is_400(self, app_client, service, zoom):
        response = app_client.get(f"{BASE}/{zoom}/9/17.webp")
        assert response.status_code == 400

    def test_tile_is_served_when_present(self, app_client, service):
        service.get_tile_data = AsyncMock(return_value=b"RIFF....WEBP")
        response = app_client.get(f"{BASE}/5/9/17.webp")
        assert response.status_code == 200
        assert response.content == b"RIFF....WEBP"

    def test_matching_etag_returns_304(self, app_client, service):
        service.get_tile_data = AsyncMock(return_value=b"RIFF....WEBP")
        first = app_client.get(f"{BASE}/5/9/17.webp")
        second = app_client.get(
            f"{BASE}/5/9/17.webp", headers={"If-None-Match": first.headers["ETag"]}
        )
        assert second.status_code == 304


class TestTileGapCaching:
    """A gap and the tile that later fills it share a URL but must not share an
    ETag, or the client's revalidation matches its own cached gap forever."""

    def test_gap_and_hit_use_different_etags(self, app_client, service):
        gap = app_client.get(f"{BASE}/5/9/17.webp")
        service.get_tile_data = AsyncMock(return_value=b"RIFF....WEBP")
        hit = app_client.get(f"{BASE}/5/9/17.webp")
        assert gap.headers["ETag"] != hit.headers["ETag"]

    def test_gap_is_not_cached_as_immutable(self, app_client, service):
        """`cache_control_tile` is 12 h + immutable; a gap is temporary."""
        response = app_client.get(f"{BASE}/5/9/17.webp")
        cache_control = response.headers["Cache-Control"]
        assert "immutable" not in cache_control
        assert "max-age=300" in cache_control

    def test_a_client_holding_the_gap_etag_still_gets_the_tile(
        self, app_client, service
    ):
        """The regression this guards: revalidating a cached gap must not 304
        once tiles-processor has written the pyramid."""
        gap_etag = app_client.get(f"{BASE}/5/9/17.webp").headers["ETag"]

        service.get_tile_data = AsyncMock(return_value=b"RIFF....WEBP")
        revalidated = app_client.get(
            f"{BASE}/5/9/17.webp", headers={"If-None-Match": gap_etag}
        )
        assert revalidated.status_code == 200
        assert revalidated.content == b"RIFF....WEBP"

    def test_a_still_missing_tile_revalidates_to_304(self, app_client, service):
        gap_etag = app_client.get(f"{BASE}/5/9/17.webp").headers["ETag"]
        again = app_client.get(
            f"{BASE}/5/9/17.webp", headers={"If-None-Match": gap_etag}
        )
        assert again.status_code == 304
        assert "max-age=300" in again.headers["Cache-Control"]


class TestOverlays:
    def test_missing_overlay_is_404(self, app_client, service):
        response = app_client.get(f"{BASE}/heights.json")
        assert response.status_code == 404

    def test_overlay_is_served_as_geojson(self, app_client, service):
        service.get_geojson = AsyncMock(return_value=b'{"type":"FeatureCollection"}')
        response = app_client.get(f"{BASE}/heights.json")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/geo+json")


class TestBarbTiles:
    def test_missing_barb_tile_is_an_empty_collection(self, app_client, service):
        """Most viewport tiles hold no barbs; 404s would flood the console."""
        response = app_client.get(f"{BASE}/barbs/4/5/9.json")
        assert response.status_code == 200
        assert response.json() == {"type": "FeatureCollection", "features": []}

    @pytest.mark.parametrize("zoom", [3, 5, 7])
    def test_non_native_barb_zoom_is_400(self, app_client, service, zoom):
        """Barbs exist only at 2/4/6/8; the frontend overzooms above that."""
        response = app_client.get(f"{BASE}/barbs/{zoom}/5/9.json")
        assert response.status_code == 400

    def test_barb_tile_is_served_when_present(self, app_client, service):
        service.get_barb_tile = AsyncMock(return_value=b'{"features":[1]}')
        response = app_client.get(f"{BASE}/barbs/4/5/9.json")
        assert response.status_code == 200
        assert response.content == b'{"features":[1]}'

    def test_matching_etag_returns_304(self, app_client, service):
        service.get_barb_tile = AsyncMock(return_value=b'{"features":[1]}')
        first = app_client.get(f"{BASE}/barbs/4/5/9.json")
        second = app_client.get(
            f"{BASE}/barbs/4/5/9.json", headers={"If-None-Match": first.headers["ETag"]}
        )
        assert second.status_code == 304

    def test_empty_collection_does_not_mask_barbs_that_appear_later(
        self, app_client, service
    ):
        empty_etag = app_client.get(f"{BASE}/barbs/4/5/9.json").headers["ETag"]

        service.get_barb_tile = AsyncMock(return_value=b'{"features":[1]}')
        revalidated = app_client.get(
            f"{BASE}/barbs/4/5/9.json", headers={"If-None-Match": empty_etag}
        )
        assert revalidated.status_code == 200
        assert revalidated.content == b'{"features":[1]}'

    def test_empty_collection_is_not_cached_as_immutable(self, app_client, service):
        response = app_client.get(f"{BASE}/barbs/4/5/9.json")
        assert "immutable" not in response.headers["Cache-Control"]
        assert "max-age=300" in response.headers["Cache-Control"]

    def test_a_still_empty_barb_tile_revalidates_to_304(self, app_client, service):
        empty_etag = app_client.get(f"{BASE}/barbs/4/5/9.json").headers["ETag"]
        again = app_client.get(
            f"{BASE}/barbs/4/5/9.json", headers={"If-None-Match": empty_etag}
        )
        assert again.status_code == 304
        assert "max-age=300" in again.headers["Cache-Control"]


class TestPointValue:
    def test_missing_cog_is_404(self, app_client):
        from services.point_value_service import (
            CogNotFoundError,
        )  # pylint: disable=C0415

        with patch("routes.gfs.point_value_service") as pvs:
            pvs.sample_gfs_point = AsyncMock(side_effect=CogNotFoundError())
            response = app_client.get(f"{BASE}/point?lat=-34&lon=-64")
        assert response.status_code == 404
        assert response.json()["detail"] == "cog_not_found"

    def test_outside_the_raster_is_404(self, app_client):
        from services.point_value_service import (  # pylint: disable=C0415
            NoDataOrOutsideError,
        )

        with patch("routes.gfs.point_value_service") as pvs:
            pvs.sample_gfs_point = AsyncMock(side_effect=NoDataOrOutsideError())
            response = app_client.get(f"{BASE}/point?lat=-34&lon=-64")
        assert response.status_code == 404
        assert response.json()["detail"] == "nodata_or_outside"

    def test_value_and_unit_are_returned(self, app_client):
        from services.point_value_service import PointSample  # pylint: disable=C0415

        with patch("routes.gfs.point_value_service") as pvs:
            pvs.sample_gfs_point = AsyncMock(
                return_value=PointSample(value=98.4, unit="kt")
            )
            response = app_client.get(f"{BASE}/point?lat=-34&lon=-64")
        assert response.status_code == 200
        body = response.json()
        assert body["value"] == 98.4
        assert body["unit"] == "kt"
        assert body["product_id"] == "500hpa"

    @pytest.mark.parametrize("query", ["lat=-91&lon=-64", "lat=-34&lon=-181"])
    def test_out_of_range_coordinates_are_422(self, app_client, query):
        assert app_client.get(f"{BASE}/point?{query}").status_code == 422
