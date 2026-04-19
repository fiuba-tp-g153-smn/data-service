"""Background scraper for pre-caching base map tiles from external providers."""

import asyncio
import logging
import time

from clients.http_tile_client import HttpTileClient
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.base_sync_service import BaseSyncService
from services.basemap_config import (
    BasemapProvider,
    build_source_url,
    get_providers,
    iter_tiles,
)
from settings import Settings

logger = logging.getLogger(__name__)


class BasemapScraperService(BaseSyncService):
    """
    Weekly background scraper that downloads base map tiles
    from external providers and stores them in S3 + Redis.
    """

    def __init__(
        self,
        settings: Settings,
        s3_client: S3Client,
        redis_client: RedisClient,
        http_client: HttpTileClient,
        tile_ttl: int,
    ):
        super().__init__(
            settings=settings,
            sync_interval=settings.basemap_scrape_interval_seconds,
            service_name="BasemapScraperService",
        )
        self._s3 = s3_client
        self._redis = redis_client
        self._http = http_client
        self._tile_ttl = tile_ttl
        self._cache_max_zoom = settings.basemap_cache_max_zoom

    def _get_lock_path(self) -> str:
        return self._settings.basemap_scrape_lock_path

    async def _run_sync(self) -> None:
        """Execute a single scrape cycle across all providers."""
        start = time.monotonic()
        total_downloaded = 0
        total_failed = 0

        for provider in get_providers().values():
            downloaded, failed = await self._scrape_provider(provider)
            total_downloaded += downloaded
            total_failed += failed

        elapsed = time.monotonic() - start
        logger.info(
            "Basemap scrape complete: %d tiles downloaded, %d failed, %.1fs elapsed",
            total_downloaded,
            total_failed,
            elapsed,
        )

    async def _scrape_provider(self, provider: BasemapProvider) -> tuple[int, int]:
        """Scrape all tiles for a single provider within the bounding box."""
        max_zoom = min(provider.cache_max_zoom, self._cache_max_zoom)
        downloaded = 0
        failed = 0

        logger.info(
            "Scraping %s (zoom %d-%d)",
            provider.provider_id,
            provider.min_zoom,
            max_zoom,
        )

        for zoom in range(provider.min_zoom, max_zoom + 1):
            for z, x, y in iter_tiles(provider, zoom):
                success = await self._download_and_store(provider, z, x, y)
                if success:
                    downloaded += 1
                else:
                    failed += 1

        logger.info(
            "Provider %s: %d downloaded, %d failed",
            provider.provider_id,
            downloaded,
            failed,
        )
        return downloaded, failed

    async def _download_and_store(
        self, provider: BasemapProvider, z: int, x: int, y: int
    ) -> bool:
        """Download a single tile from external provider and store in S3 + Redis."""
        try:
            url = build_source_url(provider, z, x, y)
            data = await self._http.download_tile(url)
            if not data:
                return False

            s3_key = S3Client.build_basemap_tile_key(provider.provider_id, z, x, y)
            await self._s3.upload_tile(s3_key, data)

            await self._redis.store_basemap_tile(
                provider.provider_id, z, x, y, data, ttl=self._tile_ttl
            )
            return True
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning(
                "Failed to scrape tile %s/%d/%d/%d: %s",
                provider.provider_id,
                z,
                x,
                y,
                exc,
            )
            return False
