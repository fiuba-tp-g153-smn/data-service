"""Service for base map tile retrieval and provider listing."""

import logging
from typing import List, Optional

from models.basemap import BasemapProviderInfo, BasemapProvidersResponse
from services.basemap_config import BasemapProvider
from services.basemap_tile_reader import BasemapTileReader

logger = logging.getLogger(__name__)


class BasemapNotConfiguredError(Exception):
    """Raised when basemap tiles are requested but the service is not configured."""


class BasemapService:
    """Service managing base map tile access.

    Instantiated once at app startup inside the FastAPI lifespan and exposed
    to routes via `Depends(get_basemap_service)`. When basemap is disabled
    (no providers, no S3), the service is still created with an empty
    registry so `/basemap/providers` answers an empty list and tile requests
    raise `BasemapNotConfiguredError`.
    """

    def __init__(
        self,
        reader: Optional[BasemapTileReader],
        providers: dict[str, BasemapProvider],
    ) -> None:
        self._reader = reader
        self._providers = providers

    def list_providers(self) -> BasemapProvidersResponse:
        """Return all available base map providers."""
        providers: List[BasemapProviderInfo] = [
            BasemapProviderInfo(
                id=p.provider_id,
                name=p.name,
                min_zoom=p.min_zoom,
                max_zoom=p.max_zoom,
                cache_max_zoom=p.cache_max_zoom,
                attribution=p.attribution,
            )
            for p in self._providers.values()
        ]
        return BasemapProvidersResponse(providers=providers)

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
