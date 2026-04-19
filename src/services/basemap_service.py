"""Service for base map tile retrieval and provider listing."""

import logging
from typing import List, Optional

from models.basemap import BasemapProviderInfo, BasemapProvidersResponse
from services.basemap_config import get_provider, get_providers
from services.basemap_sync_strategy import BasemapSyncStrategy

logger = logging.getLogger(__name__)


class BasemapService:
    """Singleton service managing base map tile access."""

    def __init__(self) -> None:
        self._strategy: Optional[BasemapSyncStrategy] = None

    def set_strategy(self, strategy: BasemapSyncStrategy) -> None:
        """Set the sync strategy (called during app startup)."""
        self._strategy = strategy

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
            for p in get_providers().values()
        ]
        return BasemapProvidersResponse(providers=providers)

    async def get_tile_data(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile bytes via the configured strategy."""
        if not self._strategy:
            return None
        return await self._strategy.get_tile(provider_id, z, x, y)

    @staticmethod
    def validate_provider(provider_id: str) -> bool:
        """Check if a provider ID is valid and enabled."""
        return get_provider(provider_id) is not None

    @staticmethod
    def validate_zoom(provider_id: str, z: int) -> bool:
        """Check if a zoom level is valid for the given provider."""
        provider = get_provider(provider_id)
        if not provider:
            return False
        return provider.min_zoom <= z <= provider.max_zoom


basemap_service = BasemapService()
