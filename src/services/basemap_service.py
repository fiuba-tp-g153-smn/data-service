"""Service for base map tile retrieval and provider listing."""

import logging
from typing import List, Optional

from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from models.basemap import BasemapProviderInfo, BasemapProvidersResponse
from services.basemap_config import BasemapProvider
from services.basemap_tile_reader import BasemapTileReader

logger = logging.getLogger(__name__)


class BasemapNotConfiguredError(Exception):
    """Raised when basemap tiles are requested but the service is not configured."""


class BasemapService:
    # pylint: disable=too-many-instance-attributes
    """Service managing base map tile access.

    Constructed once at module import as a dependency-injection singleton
    (see `dependencies.py`) and populated during the FastAPI lifespan via
    `configure(...)` — mirroring how `RedisClient` is constructed at import
    and connected in lifespan.

    Before `configure` runs (or when basemap is disabled), the service
    answers `/basemap/providers` with an empty list and raises
    `BasemapNotConfiguredError` on tile requests.
    """

    def __init__(self) -> None:
        self._reader: Optional[BasemapTileReader] = None
        self._providers: dict[str, BasemapProvider] = {}
        self._online_fallback: bool = True
        self._s3: Optional[S3Client] = None
        self._redis: Optional[RedisClient] = None
        self._presence_ttl: int = 60

    def configure(
        self,
        reader: BasemapTileReader,
        providers: dict[str, BasemapProvider],
        online_fallback: bool,
        s3_client: Optional[S3Client],
        redis_client: RedisClient,
        presence_ttl: int,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Attach runtime dependencies at startup."""
        self._reader = reader
        self._providers = providers
        self._online_fallback = online_fallback
        self._s3 = s3_client
        self._redis = redis_client
        self._presence_ttl = presence_ttl

    async def list_providers(self) -> BasemapProvidersResponse:
        """Return base map providers visible to the frontend.

        With `online_fallback` enabled, every enabled provider is returned (the
        proxy can serve misses). With fallback disabled, only providers that
        have at least one tile in the S3 cache are returned.
        """
        candidates = list(self._providers.values())
        if not self._online_fallback:
            candidates = [p for p in candidates if await self._has_cached_tiles(p)]

        providers: List[BasemapProviderInfo] = [
            BasemapProviderInfo(
                id=p.provider_id,
                name=p.name,
                min_zoom=p.min_zoom,
                max_zoom=p.max_zoom,
                cache_max_zoom=p.cache_max_zoom,
                attribution=p.attribution,
            )
            for p in candidates
        ]
        return BasemapProvidersResponse(providers=providers)

    async def _has_cached_tiles(self, provider: BasemapProvider) -> bool:
        """Whether any tile exists under basemap/<provider_id>/, cached in Redis.

        Cache-aside: check Redis, fall back to S3 `list_objects_v2(MaxKeys=1)`
        and write the boolean back with the configured TTL. Once the scraper
        writes its first tile and the TTL expires, the next call refreshes the
        flag to True.
        """
        if self._s3 is None or self._redis is None:
            return False

        cached = await self._redis.get_basemap_provider_presence(provider.provider_id)
        if cached is not None:
            return cached

        present = await self._s3.has_any_object(f"basemap/{provider.provider_id}/")
        await self._redis.set_basemap_provider_presence(
            provider.provider_id, present, self._presence_ttl
        )
        logger.debug(
            "basemap provider %s presence=%s (refreshed, ttl=%ds)",
            provider.provider_id,
            present,
            self._presence_ttl,
        )
        return present

    async def get_tile_data(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Fetch tile bytes via the configured reader.

        Raises BasemapNotConfiguredError if the service has no reader attached
        (e.g. basemap disabled at startup because S3 was unconfigured).
        """
        if not self._reader:
            raise BasemapNotConfiguredError(
                "Basemap service is not configured (no reader attached)."
            )
        return await self._reader.get_tile(provider_id, z, x, y)

    def validate_provider(self, provider_id: str) -> bool:
        """Check if a provider ID is valid and enabled."""
        return provider_id in self._providers

    def validate_zoom(self, provider_id: str, z: int) -> bool:
        """Check if a zoom level is valid for the given provider."""
        provider = self._providers.get(provider_id)
        if not provider:
            return False
        return provider.min_zoom <= z <= provider.max_zoom
