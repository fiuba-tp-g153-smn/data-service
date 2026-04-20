"""Service for ECMWF total precipitation tiles and COGs."""

import asyncio
import fcntl
import logging
from typing import List, Optional

from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from dependencies import settings
from models.base import BoundingBox, ZoomLevels
from models.ecmwf import (
    ForecastListResponse,
    ForecastRunInfo,
    PeriodInfo,
    PeriodListResponse,
)
from services.ecmwf_sync_strategy import EcmwfSyncStrategy

logger = logging.getLogger(__name__)


class EcmwfService:
    """Service managing ECMWF precipitation forecast tiles and COGs."""

    ZOOM_LEVELS = ZoomLevels(min=3, max=7)
    BOUNDING_BOX = BoundingBox(minx=-110.0, miny=-60.0, maxx=-30.0, maxy=-15.0)
    TILE_URL_PATTERN = (
        "/products/ecmwf/total-precipitation/{forecast_ts}/{period_ts}/{z}/{x}/{y}.webp"
    )

    def __init__(self) -> None:
        self._strategy: Optional[EcmwfSyncStrategy] = None
        self._s3_client: Optional[S3Client] = None
        self._redis_client: Optional[RedisClient] = None
        self._sync_task: Optional[asyncio.Task] = None

    def set_strategy(self, strategy: EcmwfSyncStrategy) -> None:
        """Set the sync strategy (called during app startup)."""
        self._strategy = strategy

    def configure_sync_clients(
        self, s3_client: S3Client, redis_client: RedisClient
    ) -> None:
        """Configure clients required for background sync (full mode only)."""
        self._s3_client = s3_client
        self._redis_client = redis_client

    async def list_forecasts(self) -> ForecastListResponse:
        """Return the N most recent forecast runs with their period counts."""
        if not self._strategy:
            return ForecastListResponse(forecasts=[])

        all_forecasts = await self._strategy.list_forecasts()
        active = all_forecasts[: settings.ecmwf_forecasts_to_keep]

        infos: List[ForecastRunInfo] = []
        for forecast_ts in active:
            periods = await self._strategy.list_periods(forecast_ts)
            infos.append(
                ForecastRunInfo(forecast_ts=forecast_ts, period_count=len(periods))
            )

        return ForecastListResponse(forecasts=infos)

    async def list_periods(self, forecast_ts: str) -> Optional[PeriodListResponse]:
        """Return all periods for a forecast run, or None if not found."""
        if not self._strategy:
            return None

        all_forecasts = await self._strategy.list_forecasts()
        if forecast_ts not in all_forecasts[: settings.ecmwf_forecasts_to_keep]:
            return None

        periods = await self._strategy.list_periods(forecast_ts)
        return PeriodListResponse(
            forecast_ts=forecast_ts,
            periods=[PeriodInfo(period_ts=p) for p in periods],
            tile_url_pattern=self.TILE_URL_PATTERN,
            zoom_levels=self.ZOOM_LEVELS,
            bounding_box=self.BOUNDING_BOX,
        )

    async def get_tile_data(
        self, forecast_ts: str, period_ts: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get tile bytes via the configured strategy."""
        if not self._strategy:
            return None
        return await self._strategy.get_tile(forecast_ts, period_ts, z, x, y)

    # ── Background sync (full mode only) ──────────────────────────────────

    async def start_sync(self, sync_logger: logging.Logger) -> None:
        """Start the background sync task."""
        sync_logger.info("Starting ECMWF sync task")
        self._sync_task = asyncio.create_task(self._sync_loop())

    async def stop_sync(self, sync_logger: logging.Logger) -> None:
        """Cancel the background sync task."""
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
            sync_logger.info("ECMWF sync task stopped")

    async def _sync_loop(self) -> None:
        while True:
            try:
                await self._sync_once()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error("ECMWF sync error: %s", exc)
            await asyncio.sleep(settings.ecmwf_sync_interval_seconds)

    async def _sync_once(self) -> None:
        s3_client = self._s3_client
        redis_client = self._redis_client
        if s3_client is None or redis_client is None:
            logger.warning("ECMWF sync skipped: S3 or Redis client not configured")
            return

        try:
            lock_file = open(
                settings.ecmwf_lock_path, "w", encoding="utf-8"
            )  # noqa: WPS515
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return  # Another worker holds the lock

        try:
            await self._do_sync(s3_client, redis_client)
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()

    async def _do_sync(self, s3_client: S3Client, redis_client: RedisClient) -> None:
        subdirs = await s3_client.get_subdirectories(S3Client.ECMWF_TILES_PREFIX)
        all_forecasts = sorted(
            (
                s.rstrip("/").split("/")[-1]
                for s in subdirs
                if s.rstrip("/").split("/")[-1]
            ),
            reverse=True,
        )
        active_forecasts = all_forecasts[: settings.ecmwf_forecasts_to_keep]

        if not active_forecasts:
            logger.debug("ECMWF sync: no forecasts found in S3")
            return

        for forecast_ts in active_forecasts:
            period_subdirs = await s3_client.get_subdirectories(
                f"{S3Client.ECMWF_TILES_PREFIX}/{forecast_ts}"
            )
            periods = sorted(
                s.rstrip("/").split("/")[-1]
                for s in period_subdirs
                if s.rstrip("/").split("/")[-1]
            )

            known_periods = await redis_client.get_ecmwf_periods(forecast_ts)
            new_periods = [p for p in periods if p not in known_periods]

            for period_ts in new_periods:
                stored = await s3_client.sync_ecmwf_period_to_redis(
                    redis_client, forecast_ts, period_ts, settings.ecmwf_tile_ttl
                )
                logger.info(
                    "ECMWF synced %d tiles for %s/%s", stored, forecast_ts, period_ts
                )

            await redis_client.store_ecmwf_index(
                forecast_ts, periods, settings.ecmwf_tile_ttl
            )

        logger.debug("ECMWF sync complete: %d forecasts active", len(active_forecasts))


ecmwf_service = EcmwfService()
