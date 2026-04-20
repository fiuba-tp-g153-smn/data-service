"""Background scraper that builds the basemap tile backup (full sync, mandatory)."""

import asyncio
import logging
import time
from logging import Logger
from typing import List, Set, Tuple

import httpx

from clients.basemap_state_store import BasemapStateStore
from clients.http_tile_client import HttpTileClient
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.base_sync_service import BaseSyncService
from services.basemap_config import (
    BasemapProvider,
    BoundingBox,
    build_source_url,
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
    # pylint: disable=too-many-instance-attributes
    """
    Periodic full-sync scraper with resumable progress tracking.

    Walks the "bounding_box x zoom" range for every enabled provider and
    writes tiles into S3 + Redis. Progress is persisted to a SQLite cold
    store so process restarts resume where the previous run left off,
    and tiles that failed download are retried on the next cycle.
    A fully-completed sweep clears all persistent state, so the next
    interval-triggered cycle starts as a fresh full scrape.

    Driven by `settings.basemap_sync_mode` (independent of the global
    `sync_mode`, which only controls satellite/radar/ECMWF). Runs in
    ``"full"``, ``"on_demand"``, and ``"no_cache"`` — only ``"relay_only"``
    skips the scraper entirely. The `redis_writes_enabled` flag controls
    whether the scraper populates Redis during the sweep; it's only True
    in ``"full"`` mode.
    """

    def __init__(
        self,
        settings: Settings,
        s3_client: S3Client,
        redis_client: RedisClient,
        http_client: HttpTileClient,
        state_store: BasemapStateStore,
        providers: dict[str, BasemapProvider],
        bbox: BoundingBox,
        tile_ttl: int,
        redis_writes_enabled: bool = True,
    ):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        super().__init__(
            settings=settings,
            sync_interval=settings.basemap_scrape_interval_seconds,
            service_name="BasemapScraperService",
        )
        self._s3 = s3_client
        self._redis = redis_client
        self._http = http_client
        self._state = state_store
        self._providers = providers
        self._bbox = bbox
        self._tile_ttl = tile_ttl
        self._redis_writes_enabled = redis_writes_enabled
        self._cache_max_zoom = settings.basemap_cache_max_zoom
        self._checkpoint_every = settings.basemap_scrape_checkpoint_every
        self._checkpoint_seconds = settings.basemap_scrape_checkpoint_seconds

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

    def _log_started(self, app_logger: Logger) -> None:
        """Log a status summary alongside the default started message."""
        app_logger.info("%s started", self._service_name)
        asyncio.create_task(self._log_startup_summary(app_logger))

    async def _log_startup_summary(self, app_logger: Logger) -> None:
        """Emit a one-line summary of which providers are due / waiting."""
        try:
            now = int(time.time())
            interval = self._sync_interval
            due = 0
            waiting = 0
            soonest_remaining: float = float(interval)
            for pid in self._providers:
                if await self._state.get_cursor(pid) is not None:
                    due += 1
                    soonest_remaining = 0.0
                    continue
                last = await self._state.get_last_completed(pid)
                remaining = (
                    float(interval)
                    if last is None
                    else max(0.0, (last + interval) - now)
                )
                if remaining <= 0:
                    due += 1
                else:
                    waiting += 1
                soonest_remaining = min(soonest_remaining, remaining)
            app_logger.info(
                "Basemap scraper: %d provider(s) due, %d waiting (next due in %s)",
                due,
                waiting,
                _fmt_duration(soonest_remaining),
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            app_logger.warning("Basemap startup summary failed: %s", exc)

    async def _compute_next_sleep(self, default: float) -> float:
        """Sleep only until the soonest-due provider, floored at 60s."""
        now = int(time.time())
        interval = self._sync_interval
        soonest = default
        for pid in self._providers:
            if await self._state.get_cursor(pid) is not None:
                return 0.0
            last = await self._state.get_last_completed(pid)
            remaining = (
                float(interval) if last is None else max(0.0, (last + interval) - now)
            )
            soonest = min(soonest, remaining)
        return max(60.0, soonest)

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

        cursor = await self._state.get_cursor(provider.provider_id)
        if cursor is None:
            last_completed = await self._state.get_last_completed(provider.provider_id)
            if last_completed is not None:
                remaining = (last_completed + self._sync_interval) - int(time.time())
                if remaining > 0:
                    logger.info(
                        "Skipping %s: next scrape in %s",
                        provider.provider_id,
                        _fmt_duration(remaining),
                    )
                    return 0, 0

        zoom_start = cursor.zoom if cursor else provider.min_zoom
        index_start = cursor.tile_index if cursor else 0

        if cursor:
            logger.info(
                "Scraping %s (zoom %d-%d) — resuming at z=%d, index=%d",
                provider.provider_id,
                provider.min_zoom,
                max_zoom,
                zoom_start,
                index_start,
            )
        else:
            logger.info(
                "Scraping %s (zoom %d-%d)",
                provider.provider_id,
                provider.min_zoom,
                max_zoom,
            )

        for zoom in range(zoom_start, max_zoom + 1):
            resume_index = index_start if zoom == zoom_start else 0
            zoom_ok, zoom_failed = await self._scrape_zoom(provider, zoom, resume_index)
            downloaded += zoom_ok
            failed += zoom_failed

        # Provider fully scraped — clear resume state and stamp completion so
        # the next cycle respects the configured scrape interval.
        await self._state.clear_cursor(provider.provider_id)
        await self._state.clear_failed_for_provider(provider.provider_id)
        await self._state.set_last_completed(provider.provider_id, int(time.time()))

        logger.info(
            "Provider %s: %d downloaded, %d failed",
            provider.provider_id,
            downloaded,
            failed,
        )
        return downloaded, failed

    async def _scrape_zoom(
        self, provider: BasemapProvider, zoom: int, resume_index: int
    ) -> tuple[int, int]:
        """Scrape one zoom level for a provider with retry + checkpointing."""
        retry_ok, retry_failed = await self._retry_failed_tiles(provider, zoom)

        coords = list(iter_tiles(zoom, self._bbox))
        total = len(coords)
        if resume_index > 0:
            logger.info(
                "%s z=%d: resuming at index %d/%d",
                provider.provider_id,
                zoom,
                resume_index,
                total,
            )
        else:
            logger.info(
                "%s z=%d: starting (%d tiles in bbox)",
                provider.provider_id,
                zoom,
                total,
            )

        sweep_ok, sweep_failed = await self._run_indexed_sweep(
            provider, zoom, coords, resume_index
        )

        # End-of-zoom cleanup: if no failures remain, drop the zoom's failed rows.
        remaining = await self._state.list_failed(provider.provider_id, zoom)
        if not remaining:
            await self._state.clear_failed(provider.provider_id, zoom)

        ok = retry_ok + sweep_ok
        failed = retry_failed + sweep_failed
        processed = total  # count_tiles-equivalent; for the final log line
        logger.info(
            "%s z=%d: done (%d tiles swept, %d ok, %d failed incl. %d retry-hits)",
            provider.provider_id,
            zoom,
            processed,
            ok,
            failed,
            retry_ok,
        )
        return ok, failed

    async def _retry_failed_tiles(
        self, provider: BasemapProvider, zoom: int
    ) -> tuple[int, int]:
        """Drain previously-failed tiles for this (provider, zoom). Returns (ok, failed)."""
        failed_tiles = await self._state.list_failed(provider.provider_id, zoom)
        if not failed_tiles:
            return 0, 0

        logger.info(
            "%s z=%d: retrying %d previously-failed tiles",
            provider.provider_id,
            zoom,
            len(failed_tiles),
        )
        ok = 0
        failed = 0
        for x, y in failed_tiles:
            if await self._download_and_store(provider, zoom, x, y):
                await self._state.remove_failed(provider.provider_id, zoom, x, y)
                ok += 1
            else:
                failed += 1
        return ok, failed

    async def _run_indexed_sweep(
        self,
        provider: BasemapProvider,
        zoom: int,
        coords: List[Tuple[int, int, int]],
        resume_index: int,
    ) -> tuple[int, int]:
        """Fan out the main sweep with watermark-based cursor checkpointing."""
        # pylint: disable=too-many-locals
        total = len(coords)
        if resume_index >= total:
            # Nothing left at this zoom; advance cursor to next zoom boundary.
            await self._state.set_cursor(provider.provider_id, zoom + 1, 0)
            return 0, 0

        start = time.monotonic()
        ok = 0
        failed = 0
        processed = 0
        next_pct = _PROGRESS_PCT_STEP
        next_time = start + _PROGRESS_TIME_INTERVAL_S
        last_log = start

        watermark = resume_index
        done_above: Set[int] = set()
        last_flushed = watermark
        last_flush_time = start

        tasks = [
            asyncio.create_task(self._download_indexed(provider, idx, z, x, y))
            for idx, (z, x, y) in enumerate(coords)
            if idx >= resume_index
        ]

        try:
            for fut in asyncio.as_completed(tasks):
                idx, z_done, x_done, y_done, was_ok = await fut
                if was_ok:
                    ok += 1
                else:
                    failed += 1
                    await self._state.add_failed(
                        provider.provider_id, z_done, x_done, y_done
                    )
                processed += 1

                watermark, flushed = await self._advance_watermark(
                    provider,
                    zoom,
                    idx,
                    watermark,
                    done_above,
                    last_flushed,
                    last_flush_time,
                )
                if flushed:
                    last_flushed = watermark
                    last_flush_time = time.monotonic()

                now = time.monotonic()
                pct = (
                    (processed * 100 // (total - resume_index))
                    if total > resume_index
                    else 100
                )
                pct_due = pct >= next_pct
                time_due = now >= next_time
                if (
                    (pct_due or time_due)
                    and processed < (total - resume_index)
                    and now - last_log >= _PROGRESS_MIN_INTERVAL_S
                ):
                    self._log_zoom_progress(
                        provider, zoom, resume_index + processed, total, now - start
                    )
                    while next_pct <= pct:
                        next_pct += _PROGRESS_PCT_STEP
                    next_time = now + _PROGRESS_TIME_INTERVAL_S
                    last_log = now
        finally:
            # Cancellation-safe: checkpoint the current watermark before
            # propagating CancelledError. Also advances cursor to next zoom
            # on a clean finish (watermark == total).
            next_cursor_zoom = zoom + 1 if watermark >= total else zoom
            next_cursor_index = 0 if watermark >= total else watermark
            await self._state.set_cursor(
                provider.provider_id, next_cursor_zoom, next_cursor_index
            )

        elapsed = time.monotonic() - start
        rate = processed / elapsed if elapsed > 0 else 0.0
        logger.info(
            "%s z=%d: swept %d (%d ok, %d failed, %s, %.1f tiles/s)",
            provider.provider_id,
            zoom,
            processed,
            ok,
            failed,
            _fmt_duration(elapsed),
            rate,
        )
        return ok, failed

    async def _advance_watermark(
        self,
        provider: BasemapProvider,
        zoom: int,
        idx: int,
        watermark: int,
        done_above: Set[int],
        last_flushed: int,
        last_flush_time: float,
    ) -> Tuple[int, bool]:
        # pylint: disable=too-many-arguments
        """
        Advance the watermark past all consecutively-completed indices and,
        if enough progress has accumulated, flush the cursor to SQLite.

        Returns `(new_watermark, flushed)`.
        """
        if idx == watermark:
            watermark += 1
            while watermark in done_above:
                done_above.remove(watermark)
                watermark += 1
        elif idx > watermark:
            done_above.add(idx)
        # idx < watermark would mean a duplicate; ignore.

        advanced = watermark - last_flushed
        elapsed_since_flush = time.monotonic() - last_flush_time
        should_flush = (
            advanced >= self._checkpoint_every
            or elapsed_since_flush >= self._checkpoint_seconds
        ) and advanced > 0

        if should_flush:
            await self._state.set_cursor(provider.provider_id, zoom, watermark)
            return watermark, True
        return watermark, False

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

    async def _download_indexed(
        self, provider: BasemapProvider, idx: int, z: int, x: int, y: int
    ) -> Tuple[int, int, int, int, bool]:
        # pylint: disable=too-many-arguments
        """Wrap `_download_and_store` so completions carry their absolute index."""
        ok = await self._download_and_store(provider, z, x, y)
        return idx, z, x, y, ok

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

            if self._redis_writes_enabled:
                await self._redis.store_basemap_tile(
                    provider.provider_id, z, x, y, data, ttl=self._tile_ttl
                )
                await self._redis.clear_basemap_tile_miss(provider.provider_id, z, x, y)
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
