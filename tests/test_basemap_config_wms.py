"""Unit tests for the WMS branch of basemap_config.

Covers the math of `tile_bbox_3857`, the GetMap URL shape produced by
`build_source_url` for `ProviderKind.WMS`, and the env-var override for
the WMS workspace URL exposed via `_load_provider`.
"""

from urllib.parse import parse_qs, urlsplit

import pytest

from services.basemap_config import (
    BasemapProvider,
    ProviderKind,
    WmsParams,
    _load_provider,
    build_source_url,
    tile_bbox_3857,
)

_WEB_MERCATOR_HALF_EXTENT = 20037508.342789244


def _wms_provider(provider_id: str = "ign-test") -> BasemapProvider:
    return BasemapProvider(
        provider_id=provider_id,
        name="IGN Test",
        source_url_template="",
        is_tms=False,
        min_zoom=3,
        max_zoom=18,
        cache_max_zoom=11,
        attribution="",
        kind=ProviderKind.WMS,
        wms=WmsParams(
            layer_name="ign:foo_layer",
            workspace_url="https://wms.example.test/geoserver/ows",
        ),
    )


# ---------- tile_bbox_3857 ----------


def test_tile_bbox_3857_z0_covers_full_web_mercator_world():
    """At zoom 0 the single tile must cover the entire EPSG:3857 extent."""
    min_x, min_y, max_x, max_y = tile_bbox_3857(0, 0, 0)
    assert min_x == pytest.approx(-_WEB_MERCATOR_HALF_EXTENT)
    assert max_x == pytest.approx(_WEB_MERCATOR_HALF_EXTENT)
    assert min_y == pytest.approx(-_WEB_MERCATOR_HALF_EXTENT)
    assert max_y == pytest.approx(_WEB_MERCATOR_HALF_EXTENT)


def test_tile_bbox_3857_z1_quadrants_partition_the_world():
    """The four z=1 tiles tile the world without gaps or overlaps."""
    nw = tile_bbox_3857(1, 0, 0)
    ne = tile_bbox_3857(1, 1, 0)
    sw = tile_bbox_3857(1, 0, 1)
    se = tile_bbox_3857(1, 1, 1)

    # NW shares its east edge with NE's west edge, etc.
    assert nw[2] == pytest.approx(ne[0])
    assert sw[2] == pytest.approx(se[0])
    assert nw[1] == pytest.approx(sw[3])
    assert ne[1] == pytest.approx(se[3])

    # XYZ convention: y=0 sits at the top, so NW must have positive max_y.
    assert nw[3] == pytest.approx(_WEB_MERCATOR_HALF_EXTENT)
    assert sw[1] == pytest.approx(-_WEB_MERCATOR_HALF_EXTENT)


# ---------- build_source_url for WMS ----------


def test_build_source_url_wms_returns_getmap_with_expected_params():
    provider = _wms_provider()
    url = build_source_url(provider, z=4, x=5, y=9)

    split = urlsplit(url)
    assert split.scheme == "https"
    assert split.netloc == "wms.example.test"
    assert split.path == "/geoserver/ows"

    params = parse_qs(split.query)
    assert params["service"] == ["WMS"]
    assert params["version"] == ["1.3.0"]
    assert params["request"] == ["GetMap"]
    assert params["layers"] == ["ign:foo_layer"]
    assert params["format"] == ["image/png"]
    assert params["transparent"] == ["true"]
    assert params["crs"] == ["EPSG:3857"]
    assert params["width"] == ["256"]
    assert params["height"] == ["256"]

    # BBOX must match tile_bbox_3857 exactly: easting,northing axis order.
    expected = tile_bbox_3857(4, 5, 9)
    bbox = [float(v) for v in params["bbox"][0].split(",")]
    assert bbox == pytest.approx(list(expected))


def test_build_source_url_wms_uses_question_mark_when_workspace_has_no_query():
    provider = _wms_provider()
    url = build_source_url(provider, z=3, x=0, y=0)
    assert url.startswith("https://wms.example.test/geoserver/ows?service=WMS")


def test_build_source_url_wms_uses_ampersand_when_workspace_already_has_query():
    provider = BasemapProvider(
        provider_id="ign-test",
        name="IGN Test",
        source_url_template="",
        is_tms=False,
        min_zoom=3,
        max_zoom=18,
        cache_max_zoom=11,
        attribution="",
        kind=ProviderKind.WMS,
        wms=WmsParams(
            layer_name="x:y",
            workspace_url="https://wms.example.test/geoserver/ows?token=abc",
        ),
    )
    url = build_source_url(provider, z=3, x=0, y=0)
    assert "?token=abc&service=WMS" in url


def test_build_source_url_xyz_path_unchanged():
    """Adding the WMS branch must not regress the XYZ path."""
    xyz_provider = BasemapProvider(
        provider_id="xyz-test",
        name="XYZ",
        source_url_template="https://example.test/{z}/{x}/{y}.png",
        is_tms=False,
        min_zoom=3,
        max_zoom=18,
        cache_max_zoom=11,
        attribution="",
    )
    assert build_source_url(xyz_provider, 4, 5, 9) == "https://example.test/4/5/9.png"


# ---------- _load_provider for WMS ----------


def test_load_provider_returns_ign_wms_provider_without_env_var(monkeypatch):
    """IGN WMS providers don't need BASEMAP_<ID>_URL — workspace URL is hardcoded."""
    # Make sure no XYZ env var bleeds into the WMS path.
    monkeypatch.delenv("BASEMAP_IGN-PROVINCIA_URL", raising=False)
    monkeypatch.delenv("BASEMAP_IGN-PROVINCIA_WORKSPACE_URL", raising=False)

    provider = _load_provider("ign-provincia")
    assert provider is not None
    assert provider.kind is ProviderKind.WMS
    assert provider.wms is not None
    assert provider.wms.layer_name == "provincia_FA003"
    assert provider.wms.workspace_url == "https://wms.ign.gob.ar/geoserver/limites/wms"


def test_load_provider_honors_wms_workspace_url_env_override(monkeypatch):
    """Operators can repoint WMS providers at a staging GeoServer via env."""
    monkeypatch.setenv(
        "BASEMAP_IGN-PROVINCIA_WORKSPACE_URL",
        "https://staging.example.test/wms",
    )
    provider = _load_provider("ign-provincia")
    assert provider is not None
    assert provider.wms is not None
    assert provider.wms.workspace_url == "https://staging.example.test/wms"
