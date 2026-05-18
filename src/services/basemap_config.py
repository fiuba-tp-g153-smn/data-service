"""Base map provider configuration and tile math utilities."""

import logging
import math
import os
from dataclasses import dataclass
from enum import Enum
from typing import Iterator, List, Optional, Tuple
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

# Web Mercator half-extent in meters (EPSG:3857). Used to map XYZ tiles to
# GetMap BBOX values for WMS providers.
_WEB_MERCATOR_HALF_EXTENT = 20037508.342789244


class ProviderKind(str, Enum):
    """Discriminator for how a basemap provider's source URL is built."""

    XYZ = "xyz"
    WMS = "wms"


@dataclass(frozen=True, slots=True)
class WmsParams:
    """WMS GetMap parameters for a provider whose kind is ``ProviderKind.WMS``.

    The scraper and reader build GetMap URLs in EPSG:3857 with the tile-grid
    BBOX so each XYZ ``(z, x, y)`` maps deterministically to one upstream
    request.
    """

    layer_name: str
    workspace_url: str
    image_format: str = "image/png"
    wms_version: str = "1.3.0"


@dataclass(frozen=True, slots=True)
# pylint: disable-next=too-many-instance-attributes
class BasemapProvider:
    """Configuration for a single base map tile provider.

    ``kind`` selects the upstream URL shape: ``XYZ`` uses the
    ``{z}/{x}/{y}`` template in ``source_url_template``; ``WMS`` ignores
    that template and uses ``wms`` to build a GetMap call. The rest of
    the pipeline (S3 keying, Redis caching, circuit breaker, prod-first
    reader) is kind-agnostic.

    ``is_overlay`` marks transparent layers that are scraped and served
    like basemaps but must NOT appear in the basemap-picker listing.
    """

    provider_id: str
    name: str
    source_url_template: str
    is_tms: bool
    min_zoom: int
    max_zoom: int
    cache_max_zoom: int
    attribution: str
    kind: ProviderKind = ProviderKind.XYZ
    wms: Optional[WmsParams] = None
    is_overlay: bool = False


@dataclass(frozen=True, slots=True)
class BoundingBox:
    """Geographic bounding box for tile scraping (degrees)."""

    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float


@dataclass(frozen=True, slots=True)
class ProviderDefaults:
    """Hardcoded provider metadata; URL comes from env at load time."""

    name: str
    is_tms: bool
    min_zoom: int
    max_zoom: int
    cache_max_zoom: int
    attribution: str
    kind: ProviderKind = ProviderKind.XYZ
    wms: Optional[WmsParams] = None
    is_overlay: bool = False


PROVIDER_DEFAULTS: dict[str, ProviderDefaults] = {
    "argenmap": ProviderDefaults(
        name="Argenmap",
        is_tms=True,
        min_zoom=3,
        max_zoom=21,
        cache_max_zoom=11,
        attribution="Instituto Geográfico Nacional + OpenStreetMap contributors",
    ),
    "argenmapGris": ProviderDefaults(
        name="Argenmap gris",
        is_tms=True,
        min_zoom=3,
        max_zoom=21,
        cache_max_zoom=11,
        attribution="Instituto Geográfico Nacional",
    ),
    "argenmapOscuro": ProviderDefaults(
        name="Argenmap oscuro",
        is_tms=True,
        min_zoom=3,
        max_zoom=21,
        cache_max_zoom=11,
        attribution="Instituto Geográfico Nacional",
    ),
    "argenmapTopografico": ProviderDefaults(
        name="Argenmap topográfico",
        is_tms=True,
        min_zoom=3,
        max_zoom=21,
        cache_max_zoom=11,
        attribution="Instituto Geográfico Nacional",
    ),
    "satellite": ProviderDefaults(
        name="Imágenes satelitales Esri",
        is_tms=False,
        min_zoom=3,
        max_zoom=17,
        cache_max_zoom=11,
        attribution="Tiles © Esri",
    ),
    "topographic": ProviderDefaults(
        name="Mapa topográfico Esri",
        is_tms=False,
        min_zoom=3,
        max_zoom=8,
        cache_max_zoom=8,
        attribution="Tiles © Esri",
    ),
    "googleSatellite": ProviderDefaults(
        name="Imágenes satelitales Google",
        is_tms=False,
        min_zoom=3,
        max_zoom=20,
        cache_max_zoom=11,
        attribution="© Google",
    ),
    "oceanBase": ProviderDefaults(
        name="Mapa Esri Fondo Oceánico",
        is_tms=False,
        min_zoom=3,
        max_zoom=16,
        cache_max_zoom=11,
        attribution="Tiles © Esri",
    ),
    # IGN WMS reference overlays. Served from wms.ign.gob.ar as GetMap calls
    # translated from the standard XYZ tile grid in EPSG:3857. These are
    # transparent overlays meant to render on top of a raster basemap, so the
    # frontend instantiates them as overlay layers (not base layers).
    "ign-provincia": ProviderDefaults(
        name="Provincia (IGN)",
        is_tms=False,
        min_zoom=3,
        max_zoom=18,
        cache_max_zoom=11,
        attribution="Instituto Geográfico Nacional",
        kind=ProviderKind.WMS,
        wms=WmsParams(
            layer_name="provincia_FA003",
            workspace_url="https://wms.ign.gob.ar/geoserver/limites/wms",
        ),
        is_overlay=True,
    ),
    "ign-limite-internacional": ProviderDefaults(
        name="Límite internacional (IGN)",
        is_tms=False,
        min_zoom=3,
        max_zoom=18,
        cache_max_zoom=11,
        attribution="Instituto Geográfico Nacional",
        kind=ProviderKind.WMS,
        wms=WmsParams(
            layer_name="linea_de_limite_FA004",
            workspace_url="https://wms.ign.gob.ar/geoserver/limites/wms",
        ),
        is_overlay=True,
    ),
    "ign-limite-interdepartamental-o-de-partido": ProviderDefaults(
        name="Límite interdepartamental o de partido (IGN)",
        is_tms=False,
        min_zoom=4,
        max_zoom=18,
        cache_max_zoom=11,
        attribution="Instituto Geográfico Nacional",
        kind=ProviderKind.WMS,
        wms=WmsParams(
            layer_name="ign:linea_de_limite_070110",
            workspace_url="https://wms.ign.gob.ar/geoserver/ows",
        ),
        is_overlay=True,
    ),
    "ign-localidad": ProviderDefaults(
        name="Localidad (IGN)",
        is_tms=False,
        min_zoom=5,
        max_zoom=18,
        cache_max_zoom=11,
        attribution="Instituto Geográfico Nacional",
        kind=ProviderKind.WMS,
        wms=WmsParams(
            layer_name="ign:localidad_bahra",
            workspace_url="https://wms.ign.gob.ar/geoserver/ows",
        ),
        is_overlay=True,
    ),
    "ign-sublocalidad": ProviderDefaults(
        name="Sublocalidad (IGN)",
        is_tms=False,
        min_zoom=7,
        max_zoom=18,
        cache_max_zoom=11,
        attribution="Instituto Geográfico Nacional",
        kind=ProviderKind.WMS,
        wms=WmsParams(
            layer_name="ign:sublocalidad_entidad_bahra",
            workspace_url="https://wms.ign.gob.ar/geoserver/ows",
        ),
        is_overlay=True,
    ),
    "ign-gobierno-local": ProviderDefaults(
        name="Gobierno Local (IGN)",
        is_tms=False,
        min_zoom=5,
        max_zoom=18,
        cache_max_zoom=11,
        attribution="Instituto Geográfico Nacional",
        kind=ProviderKind.WMS,
        wms=WmsParams(
            layer_name="gobiernoslocales_2022",
            workspace_url="https://wms.ign.gob.ar/geoserver/limites/wms",
        ),
        is_overlay=True,
    ),
}


def _env_prefix(provider_id: str) -> str:
    """Build env var prefix for a provider ID (e.g. 'argenmapGris' -> 'BASEMAP_ARGENMAPGRIS')."""
    return f"BASEMAP_{provider_id.upper()}"


def _load_provider(provider_id: str) -> Optional[BasemapProvider]:
    """Merge hardcoded defaults with runtime configuration.

    XYZ providers require a ``BASEMAP_<ID>_URL`` env var (kept out of the
    repo because some endpoints embed API keys). WMS providers carry their
    upstream URL in :class:`WmsParams`; an optional
    ``BASEMAP_<ID>_WORKSPACE_URL`` env var lets ops point them at a staging
    GeoServer without code changes.
    """
    defaults = PROVIDER_DEFAULTS.get(provider_id)
    if not defaults:
        logger.warning("No defaults for basemap provider '%s'; skipping", provider_id)
        return None

    if defaults.kind is ProviderKind.WMS:
        if defaults.wms is None:
            logger.warning(
                "WMS basemap provider '%s' missing WmsParams in defaults; skipping",
                provider_id,
            )
            return None
        workspace_url = os.getenv(
            f"{_env_prefix(provider_id)}_WORKSPACE_URL", defaults.wms.workspace_url
        )
        wms = WmsParams(
            layer_name=defaults.wms.layer_name,
            workspace_url=workspace_url,
            image_format=defaults.wms.image_format,
            wms_version=defaults.wms.wms_version,
        )
        return BasemapProvider(
            provider_id=provider_id,
            name=defaults.name,
            source_url_template="",
            is_tms=defaults.is_tms,
            min_zoom=defaults.min_zoom,
            max_zoom=defaults.max_zoom,
            cache_max_zoom=defaults.cache_max_zoom,
            attribution=defaults.attribution,
            kind=ProviderKind.WMS,
            wms=wms,
            is_overlay=defaults.is_overlay,
        )

    url = os.getenv(f"{_env_prefix(provider_id)}_URL", "")
    if not url:
        logger.warning(
            "Basemap provider '%s' enabled but %s_URL not set; skipping",
            provider_id,
            _env_prefix(provider_id),
        )
        return None

    return BasemapProvider(
        provider_id=provider_id,
        name=defaults.name,
        source_url_template=url,
        is_tms=defaults.is_tms,
        min_zoom=defaults.min_zoom,
        max_zoom=defaults.max_zoom,
        cache_max_zoom=defaults.cache_max_zoom,
        attribution=defaults.attribution,
        is_overlay=defaults.is_overlay,
    )


def load_providers(config_list: List[dict]) -> dict[str, BasemapProvider]:
    """
    Build the enabled-provider registry from settings.json toggles.

    Each entry has {id, enabled}. URL comes from BASEMAP_<UPPER_ID>_URL env var;
    other metadata (name, TMS flag, zoom range, attribution) from PROVIDER_DEFAULTS.
    Returns a dict keyed by provider_id; caller owns storage and lifecycle.
    """
    providers: dict[str, BasemapProvider] = {}
    for cfg in config_list:
        if not cfg.get("enabled", True):
            continue
        provider_id = cfg.get("id")
        if not provider_id:
            continue

        provider = _load_provider(provider_id)
        if provider:
            providers[provider.provider_id] = provider

    logger.info(
        "Loaded %d enabled basemap providers: %s",
        len(providers),
        ", ".join(providers.keys()) or "(none)",
    )
    return providers


def lon_to_tile_x(lon: float, zoom: int) -> int:
    """Convert longitude to tile X coordinate."""
    return int((lon + 180.0) / 360.0 * (1 << zoom))


def lat_to_tile_y(lat: float, zoom: int) -> int:
    """Convert latitude to tile Y coordinate (XYZ convention, Y=0 at top)."""
    lat_rad = math.radians(lat)
    n = 1 << zoom
    return int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)


def tms_y_flip(y: int, zoom: int) -> int:
    """Convert XYZ Y coordinate to TMS Y coordinate."""
    return (1 << zoom) - 1 - y


def _tile_range(zoom: int, bbox: BoundingBox) -> Tuple[int, int, int, int]:
    """Return (x_min, x_max, y_min, y_max) tile bounds clamped to world extent."""
    x_min = lon_to_tile_x(bbox.lon_min, zoom)
    x_max = lon_to_tile_x(bbox.lon_max, zoom)
    y_min = lat_to_tile_y(bbox.lat_max, zoom)
    y_max = lat_to_tile_y(bbox.lat_min, zoom)

    max_tile = (1 << zoom) - 1
    return (
        max(0, x_min),
        min(max_tile, x_max),
        max(0, y_min),
        min(max_tile, y_max),
    )


def iter_tiles(zoom: int, bbox: BoundingBox) -> Iterator[Tuple[int, int, int]]:
    """Yield all (z, x, y) tile coordinates within the bounding box for a zoom level."""
    x_min, x_max, y_min, y_max = _tile_range(zoom, bbox)
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            yield zoom, x, y


def count_tiles(zoom: int, bbox: BoundingBox) -> int:
    """Return the number of tiles `iter_tiles` would yield for this zoom/bbox."""
    x_min, x_max, y_min, y_max = _tile_range(zoom, bbox)
    if x_max < x_min or y_max < y_min:
        return 0
    return (x_max - x_min + 1) * (y_max - y_min + 1)


def tile_bbox_3857(z: int, x: int, y: int) -> Tuple[float, float, float, float]:
    """Return the EPSG:3857 BBOX ``(min_x, min_y, max_x, max_y)`` of a tile.

    Uses the standard XYZ tile scheme (Y=0 at top). The returned tuple is
    in meters and matches the axis order WMS GetMap expects for EPSG:3857
    BBOX values (easting first, then northing).
    """
    tile_size = (2.0 * _WEB_MERCATOR_HALF_EXTENT) / (1 << z)
    min_x = -_WEB_MERCATOR_HALF_EXTENT + x * tile_size
    max_x = min_x + tile_size
    max_y = _WEB_MERCATOR_HALF_EXTENT - y * tile_size
    min_y = max_y - tile_size
    return min_x, min_y, max_x, max_y


def _build_wms_url(provider: BasemapProvider, z: int, x: int, y: int) -> str:
    """Build a WMS GetMap URL for a single 256x256 tile in EPSG:3857.

    Mirrors the param set Leaflet's ``L.tileLayer.wms`` would send: WMS 1.3.0
    uses ``CRS`` (not ``SRS``) and BBOX in easting/northing order for
    EPSG:3857. All values pass through ``urlencode`` so layer names that
    embed a namespace separator (``ign:foo``) survive proxies and routers.
    """
    assert provider.wms is not None  # guarded by ProviderKind.WMS dispatch
    min_x, min_y, max_x, max_y = tile_bbox_3857(z, x, y)
    params = urlencode(
        {
            "service": "WMS",
            "version": provider.wms.wms_version,
            "request": "GetMap",
            "layers": provider.wms.layer_name,
            "styles": "",
            "format": provider.wms.image_format,
            "transparent": "true",
            "crs": "EPSG:3857",
            "width": 256,
            "height": 256,
            "bbox": f"{min_x},{min_y},{max_x},{max_y}",
        }
    )
    separator = "&" if "?" in provider.wms.workspace_url else "?"
    return f"{provider.wms.workspace_url}{separator}{params}"


def build_source_url(provider: BasemapProvider, z: int, x: int, y: int) -> str:
    """Build the external source URL for a tile.

    XYZ providers substitute the canonical ``{z}/{x}/{y}`` template
    (handling TMS Y-flip). WMS providers translate the tile to a GetMap
    BBOX in EPSG:3857 against the configured workspace URL.
    """
    if provider.kind is ProviderKind.WMS:
        return _build_wms_url(provider, z, x, y)
    actual_y = tms_y_flip(y, z) if provider.is_tms else y
    return provider.source_url_template.format(z=z, x=x, y=actual_y)
