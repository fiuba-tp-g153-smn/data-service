"""Background scraper that builds the basemap tile backup (full sync, mandatory)."""

import asyncio
import logging
import time
from logging import Logger

import httpx

from clients.http_tile_client import HttpTileClient
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.base_sync_service import BaseSyncService
from services.basemap_config import (
    BasemapProvider,
    BoundingBox,
    build_source_url,
    count_tiles,
    iter_tiles,
)
from settings import Settings

_PROGRESS_PCT_STEP = 10
_PROGRESS_TIME_INTERVAL_S = 30.0
_PROGRESS_MIN_INTERVAL_S = 1.0


def _fmt_duration(seconds: float) -> str:
    """Format a duration as e.g. '0.9s', '42s', '3m22s', '1h04m'."""
    if seconds < 60:
        return f"{seconds:.1f}s" if seconds < 10 else f"{int(seconds)}s"
    if seconds < 3600:
        minutes, secs = divmod(int(seconds), 60)
        return f"{minutes}m{secs:02d}s"
    hours, rem = divmod(int(seconds), 3600)
    minutes = rem // 60
    return f"{hours}h{minutes:02d}m"


logger = logging.getLogger(__name__)


class BasemapScraperService(BaseSyncService):
    """
    Periodic full-sync scraper: walks the "bounding_box x zoom" range for every
    enabled provider and writes tiles into S3 + Redis. This is the primary
    source of basemap data — the on-request reader only falls back to the
    external provider for tiles this scraper hasn't produced yet.

    Runs independently of `settings.sync_mode` (which controls satellite /
    radar / ECMWF) because a basemap backup without full sync defeats the
    purpose of caching provider tiles locally.
    """

    def __init__(
        self,
        settings: Settings,
        s3_client: S3Client,
        redis_client: RedisClient,
        http_client: HttpTileClient,
        providers: dict[str, BasemapProvider],
        bbox: BoundingBox,
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
        self._providers = providers
        self._bbox = bbox
        self._tile_ttl = tile_ttl
        self._cache_max_zoom = settings.basemap_cache_max_zoom

    def _get_lock_path(self) -> str:
        return self._settings.basemap_scrape_lock_path

    def _pre_start_check(self, app_logger: Logger) -> bool:
        """Refuse to start when there are no enabled providers to scrape."""
        if not self._providers:
            app_logger.info(
                "%s not started: no enabled providers to scrape", self._service_name
            )
            return False
        return True

    async def _run_sync(self) -> None:
        """Execute a single scrape cycle across all providers."""
        start = time.monotonic()
        total_downloaded = 0
        total_failed = 0

        for provider in self._providers.values():
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
            zoom_ok, zoom_failed = await self._scrape_zoom(provider, zoom)
            downloaded += zoom_ok
            failed += zoom_failed

        logger.info(
            "Provider %s: %d downloaded, %d failed",
            provider.provider_id,
            downloaded,
            failed,
        )
        return downloaded, failed

    async def _scrape_zoom(
        self, provider: BasemapProvider, zoom: int
    ) -> tuple[int, int]:
        """Scrape one zoom level for a provider with throttled progress logs."""
        total = count_tiles(zoom, self._bbox)
        logger.info(
            "%s z=%d: starting (%d tiles in bbox)",
            provider.provider_id,
            zoom,
            total,
        )

        start = time.monotonic()
        ok = 0
        failed = 0
        processed = 0
        next_pct = _PROGRESS_PCT_STEP
        next_time = start + _PROGRESS_TIME_INTERVAL_S
        last_log = start

        tasks = [
            asyncio.create_task(self._download_and_store(provider, z, x, y))
            for z, x, y in iter_tiles(zoom, self._bbox)
        ]

        for fut in asyncio.as_completed(tasks):
            if await fut:
                ok += 1
            else:
                failed += 1
            processed += 1

            now = time.monotonic()
            pct = (processed * 100 // total) if total else 100
            pct_due = pct >= next_pct
            time_due = now >= next_time
            if (
                (pct_due or time_due)
                and processed < total
                and now - last_log >= _PROGRESS_MIN_INTERVAL_S
            ):
                self._log_zoom_progress(provider, zoom, processed, total, now - start)
                while next_pct <= pct:
                    next_pct += _PROGRESS_PCT_STEP
                next_time = now + _PROGRESS_TIME_INTERVAL_S
                last_log = now

        elapsed = time.monotonic() - start
        rate = processed / elapsed if elapsed > 0 else 0.0
        logger.info(
            "%s z=%d: done (%d tiles, %d ok, %d failed, %s, %.1f tiles/s)",
            provider.provider_id,
            zoom,
            processed,
            ok,
            failed,
            _fmt_duration(elapsed),
            rate,
        )
        return ok, failed

    def _log_zoom_progress(
        self,
        provider: BasemapProvider,
        zoom: int,
        processed: int,
        total: int,
        elapsed: float,
    ) -> None:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Emit one in-zoom progress line with rate + ETA."""
        pct = (processed * 100 // total) if total else 100
        rate = processed / elapsed if elapsed > 0 else 0.0
        remaining = max(total - processed, 0)
        eta = remaining / rate if rate > 0 else 0.0
        logger.info(
            "%s z=%d: %d/%d (%d%%) @ %.1f tiles/s, ETA %s",
            provider.provider_id,
            zoom,
            processed,
            total,
            pct,
            rate,
            _fmt_duration(eta),
        )

    async def _download_and_store(
        self, provider: BasemapProvider, z: int, x: int, y: int
    ) -> bool:
        """Download a single tile from the external provider and store in S3 + Redis."""
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
        except (httpx.HTTPError, asyncio.TimeoutError, OSError) as exc:
            logger.warning(
                "Failed to scrape tile %s/%d/%d/%d: %s",
                provider.provider_id,
                z,
                x,
                y,
                exc,
            )
            return False
