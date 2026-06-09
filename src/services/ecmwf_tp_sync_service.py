"""Independent background sync loop for ECMWF total precipitation tiles."""

import logging
from typing import Optional, Tuple

from clients.s3_client import S3Client
from services.domain_sync_service import DomainSyncService
from services.ecmwf_tp_sync_strategy import is_valid_timestamp_format
from settings import Settings

logger = logging.getLogger(__name__)


class EcmwfTpSyncService(DomainSyncService):
    """Syncs ECMWF total precipitation forecasts/periods from S3 on its own loop."""

    def __init__(self, settings: Optional[Settings] = None):
        resolved = settings or Settings.get_settings()
        super().__init__(
            resolved,
            domain="ecmwf_tp",
            lock_path=resolved.ecmwf_tp_sync_lock_path,
            interval=resolved.sync_interval_seconds,
            timeout=resolved.sync_domain_timeout_seconds,
            s3_concurrency=resolved.s3_max_concurrent_downloads,
            service_name="ECMWF-TP sync",
        )

    async def _run_sync(self) -> None:
        await self._run_single_domain(self._sync_ecmwf_tp())

    async def _sync_ecmwf_tp(self) -> Tuple[int, int]:
        """Sync ECMWF total precipitation from S3 to Redis. Returns (downloaded, errors)."""
        if self._client is None or self._redis_client is None:
            raise RuntimeError("S3 or Redis client is not initialized")

        total_downloaded = 0
        errors = 0

        try:
            subdirs = await self._client.get_subdirectories(
                S3Client.ECMWF_TP_TILES_PREFIX
            )
            all_forecasts = sorted(
                (
                    s.rstrip("/").split("/")[-1]
                    for s in subdirs
                    if s.rstrip("/").split("/")[-1]
                ),
                reverse=True,
            )
            active_forecasts = all_forecasts[: self._settings.ecmwf_forecasts_to_keep]

            if not active_forecasts:
                logger.debug("ECMWF-TP sync: no forecasts found in S3")
                return 0, 0

            for forecast_ts in active_forecasts:
                period_subdirs = await self._client.get_subdirectories(
                    f"{S3Client.ECMWF_TP_TILES_PREFIX}/{forecast_ts}"
                )
                periods = sorted(
                    s.rstrip("/").split("/")[-1]
                    for s in period_subdirs
                    if s.rstrip("/").split("/")[-1]
                    and is_valid_timestamp_format(s.rstrip("/").split("/")[-1])
                )

                known_periods = await self._redis_client.get_ecmwf_tp_periods(
                    forecast_ts
                )
                new_periods = [p for p in periods if p not in known_periods]

                for period_ts in new_periods:
                    stored = await self._client.sync_ecmwf_tp_period_to_redis(
                        self._redis_client,
                        forecast_ts,
                        period_ts,
                        self._settings.ecmwf_tile_ttl,
                    )
                    total_downloaded += stored
                    logger.info(
                        "ECMWF-TP synced %d tiles for %s/%s",
                        stored,
                        forecast_ts,
                        period_ts,
                    )

                await self._redis_client.store_ecmwf_tp_index(
                    forecast_ts, periods, self._settings.ecmwf_tile_ttl
                )

            # Reconcile the forecasts index to the active set so it can't
            # accumulate stale forecast timestamps over time.
            await self._redis_client.prune_ecmwf_tp_forecasts(active_forecasts)

            logger.debug(
                "ECMWF-TP sync complete: %d forecasts active", len(active_forecasts)
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("ECMWF-TP sync error: %s", e)
            errors += 1

        return total_downloaded, errors


# Singleton instance for use across the application
ecmwf_tp_sync_service = EcmwfTpSyncService()
