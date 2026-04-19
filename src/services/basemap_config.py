"""Base map provider configuration and tile math utilities."""

import logging
import math
import os
from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BasemapProvider:
    """Configuration for a single base map tile provider."""

    provider_id: str
    name: str
    source_url_template: str
    is_tms: bool
    min_zoom: int
    max_zoom: int
    cache_max_zoom: int
    attribution: str


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
}


def _env_prefix(provider_id: str) -> str:
    """Build env var prefix for a provider ID (e.g. 'argenmapGris' -> 'BASEMAP_ARGENMAPGRIS')."""
    return f"BASEMAP_{provider_id.upper()}"


def _load_provider(provider_id: str) -> Optional[BasemapProvider]:
    """Merge hardcoded defaults with URL from env var."""
    defaults = PROVIDER_DEFAULTS.get(provider_id)
    if not defaults:
        logger.warning("No defaults for basemap provider '%s'; skipping", provider_id)
        return None

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


def iter_tiles(zoom: int, bbox: BoundingBox) -> Iterator[Tuple[int, int, int]]:
    """Yield all (z, x, y) tile coordinates within the bounding box for a zoom level."""
    x_min = lon_to_tile_x(bbox.lon_min, zoom)
    x_max = lon_to_tile_x(bbox.lon_max, zoom)
    y_min = lat_to_tile_y(bbox.lat_max, zoom)
    y_max = lat_to_tile_y(bbox.lat_min, zoom)

    max_tile = (1 << zoom) - 1
    x_min = max(0, x_min)
    x_max = min(max_tile, x_max)
    y_min = max(0, y_min)
    y_max = min(max_tile, y_max)

    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            yield zoom, x, y


def build_source_url(provider: BasemapProvider, z: int, x: int, y: int) -> str:
    """Build the external source URL for a tile, handling TMS Y-flip."""
    actual_y = tms_y_flip(y, z) if provider.is_tms else y
    return provider.source_url_template.format(z=z, x=x, y=actual_y)
