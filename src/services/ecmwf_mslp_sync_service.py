"""Independent background sync loop for ECMWF mean-sea-level-pressure GeoJSON."""

import logging
from typing import List, Optional, Tuple

from clients.s3_client import S3Client
from services.domain_sync_service import DomainSyncService
from services.ecmwf_tp_sync_strategy import is_valid_timestamp_format
from settings import Settings

logger = logging.getLogger(__name__)


class EcmwfMslpSyncService(DomainSyncService):
    """Syncs ECMWF MSLP forecasts/timestamps from S3 to Redis on its own loop."""

    def __init__(self, settings: Optional[Settings] = None):
        resolved = settings or Settings.get_settings()
        super().__init__(
            resolved,
            domain="ecmwf_mslp",
            lock_path=resolved.ecmwf_mslp_sync_lock_path,
            interval=resolved.sync_interval_seconds,
            timeout=resolved.sync_domain_timeout_seconds,
            s3_concurrency=resolved.s3_max_concurrent_downloads,
            service_name="ECMWF-MSLP sync",
        )

    async def _run_sync(self) -> None:
        await self._run_single_domain(self._sync_ecmwf_mslp())

    async def _sync_ecmwf_mslp(self) -> Tuple[int, int]:
        """Sync ECMWF MSLP from S3 to Redis. Returns (downloaded, errors)."""
        if self._client is None or self._redis_client is None:
            raise RuntimeError("S3 or Redis client is not initialized")

        total_downloaded = 0
        errors = 0

        try:
            subdirs = await self._client.get_subdirectories(
                S3Client.ECMWF_MSLP_COG_PREFIX
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
                logger.debug("ECMWF-MSLP sync: no forecasts found in S3")
                return 0, 0

            for forecast_ts in active_forecasts:
                basenames = await self._client.list_object_basenames(
                    f"{S3Client.ECMWF_MSLP_COG_PREFIX}/{forecast_ts}/", ".tif"
                )
                timestamps = sorted(
                    b for b in basenames if is_valid_timestamp_format(b)
                )

                known_timestamps = await self._redis_client.get_ecmwf_mslp_timestamps(
                    forecast_ts
                )
                new_timestamps = [t for t in timestamps if t not in known_timestamps]

                stored_timestamps: List[str] = []
                if new_timestamps:
                    stored_timestamps = (
                        await self._client.sync_ecmwf_mslp_forecast_to_redis(
                            self._redis_client,
                            forecast_ts,
                            new_timestamps,
                            self._settings.ecmwf_mslp_geojson_ttl,
                        )
                    )
                    total_downloaded += len(stored_timestamps)
                    logger.info(
                        "ECMWF-MSLP synced %d geojsons for %s",
                        len(stored_timestamps),
                        forecast_ts,
                    )

                # Index only timestamps backed by a stored GeoJSON: those already
                # indexed and still present in the COG listing, plus the ones
                # actually stored this cycle. A new timestamp whose GeoJSON is
                # missing/late is left unindexed and retried next cycle, instead
                # of being advertised forever with no isobars behind it.
                timestamp_set = set(timestamps)
                indexed = sorted(
                    {t for t in known_timestamps if t in timestamp_set}
                    | set(stored_timestamps)
                )
                await self._redis_client.store_ecmwf_mslp_index(
                    forecast_ts, indexed, self._settings.ecmwf_mslp_geojson_ttl
                )

            # Reconcile the forecasts index to the active set so it can't
            # accumulate stale forecast timestamps over time.
            await self._redis_client.prune_ecmwf_mslp_forecasts(active_forecasts)

            logger.debug(
                "ECMWF-MSLP sync complete: %d forecasts active",
                len(active_forecasts),
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("ECMWF-MSLP sync error: %s", e)
            errors += 1

        return total_downloaded, errors


# Singleton instance for use across the application
ecmwf_mslp_sync_service = EcmwfMslpSyncService()
