"""
Background Sync Service.

Periodically syncs tile data from S3 bucket directly to Redis.
Runs as a background task during application lifetime.
"""

import logging
import re
import time
from logging import Logger
from typing import List, Optional

from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.base_sync_service import BaseSyncService
from services.ecmwf_tp_sync_strategy import is_centered_period_format
from settings import Settings

logger = logging.getLogger(__name__)

# S3 prefix where radar data lives
RADAR_S3_PREFIX = "tiles/radar"


class SyncService(BaseSyncService):
    """
    Background service that syncs tiles from S3 to Redis.

    Runs periodically to ensure Redis tile store stays in sync
    with the S3 bucket populated by tiles-processor.
    Uses Redis tileset index for incremental sync (only downloads
    new tilesets not already in Redis). Tiles are stored with a TTL
    so eviction is handled by Redis expiration.
    """

    # Prefixes to sync from S3 (matches tiles-processor output structure)
    DEFAULT_SYNC_PREFIXES = [
        "tiles/band_13",
        "tiles/band_9",
        "tiles/band_2",
        "tiles/glm_fed",
        "tiles/glm_toe",
        "tiles/glm_mfa",
    ]

    # Maps S3 prefix to channel_dir for Redis key construction
    PREFIX_TO_CHANNEL = {
        "tiles/band_13": "band_13",
        "tiles/band_9": "band_9",
        "tiles/band_2": "band_2",
        "tiles/glm_fed": "glm_fed",
        "tiles/glm_toe": "glm_toe",
        "tiles/glm_mfa": "glm_mfa",
    }

    def __init__(
        self,
        settings: Optional[Settings] = None,
        sync_prefixes: Optional[List[str]] = None,
    ):
        resolved_settings = settings or Settings.get_settings()
        self._sync_prefixes = sync_prefixes or self.DEFAULT_SYNC_PREFIXES
        super().__init__(
            settings=resolved_settings,
            sync_interval=resolved_settings.sync_interval_seconds,
            service_name="Sync service",
        )
        self._client: Optional[S3Client] = None
        self._redis_client: Optional[RedisClient] = None
        self._consecutive_failures = 0
        self._total_cycles = 0

    def set_redis_client(self, redis_client: RedisClient) -> None:
        """Set the Redis client (called during app startup)."""
        self._redis_client = redis_client

    def _get_lock_path(self) -> str:
        """Return the S3 sync lock file path."""
        return self._settings.sync_lock_path

    def _create_client(self) -> S3Client:
        """Create S3 client from settings."""
        return S3Client(
            endpoint=self._settings.s3_tiles_data_endpoint,
            access_key=self._settings.s3_tiles_data_access_key,
            secret_key=self._settings.s3_tiles_data_secret_key,
            bucket=self._settings.s3_tiles_data_bucket_name,
            max_concurrent_downloads=self._settings.s3_max_concurrent_downloads,
            secure=self._settings.s3_tiles_data_secure,
        )

    def _log_started(self, app_logger: Logger) -> None:
        """Log S3 sync-specific start message."""
        app_logger.info(
            "Sync service started (Lock acquired). Interval: %ss, Prefixes: %s",
            self._sync_interval,
            self._sync_prefixes,
        )

    def _on_sync_error(self, error: Exception) -> None:
        """Track consecutive failures for sync status reporting."""
        self._consecutive_failures += 1

    async def _run_sync(self) -> None:
        """Execute a single sync cycle for all prefixes."""
        if not self._settings.is_s3_configured():
            logger.warning(
                "S3 not configured. "
                "Set S3_TILES_DATA_ENDPOINT, "
                "S3_TILES_DATA_ACCESS_KEY, "
                "and S3_TILES_DATA_SECRET_KEY. "
                "Retrying in next cycle..."
            )
            return

        if not self._client:
            self._client = self._create_client()

        if not self._redis_client:
            return

        sync_start = time.time()
        await self._redis_client.update_sync_status(
            {"is_running": "true", "last_sync_start": str(sync_start)}
        )

        logger.info("Sync cycle #%d starting", self._total_cycles + 1)

        sat_downloaded, sat_errors = await self._sync_satellite_prefixes()
        radar_downloaded, radar_errors = await self._sync_radar()
        ecmwf_tp_downloaded, ecmwf_tp_errors = await self._sync_ecmwf_tp()
        ecmwf_mslp_downloaded, ecmwf_mslp_errors = await self._sync_ecmwf_mslp()

        errors = sat_errors + radar_errors + ecmwf_tp_errors + ecmwf_mslp_errors
        total_downloaded = (
            sat_downloaded
            + radar_downloaded
            + ecmwf_tp_downloaded
            + ecmwf_mslp_downloaded
        )

        self._total_cycles += 1
        if errors == 0:
            self._consecutive_failures = 0
        else:
            self._consecutive_failures += 1

        sync_end = time.time()
        duration_ms = int((sync_end - sync_start) * 1000)

        # Count total satellite tilesets across all channels
        sat_count = 0
        for prefix in self._sync_prefixes:
            channel_dir = self.PREFIX_TO_CHANNEL.get(
                prefix, prefix.rstrip("/").rsplit("/", maxsplit=1)[-1]
            )
            tilesets = await self._redis_client.get_satellite_tilesets(channel_dir)
            sat_count += len(tilesets)

        # Count radar tilesets from Redis (same pattern as satellite)
        radar_count = await self._count_radar_tilesets()

        await self._redis_client.update_sync_status(
            {
                "is_running": "false",
                "last_sync_end": str(sync_end),
                "last_sync_duration_ms": str(duration_ms),
                "last_sync_downloaded": str(total_downloaded),
                "last_sync_errors": str(errors),
                "consecutive_failures": str(self._consecutive_failures),
                "total_cycles": str(self._total_cycles),
                "satellite_tilesets_count": str(sat_count),
                "radar_tilesets_count": str(radar_count),
            }
        )

        logger.info(
            "Sync cycle #%d done in %dms | sat: %d new tiles / %d tilesets"
            " | radar: %d new tiles / %d tilesets"
            " | ecmwf-tp: %d new tiles | ecmwf-mslp: %d new geojsons"
            " | errors: %d",
            self._total_cycles,
            duration_ms,
            sat_downloaded,
            sat_count,
            radar_downloaded,
            radar_count,
            ecmwf_tp_downloaded,
            ecmwf_mslp_downloaded,
            errors,
        )
        if self._consecutive_failures > 0:
            logger.warning(
                "Sync has %d consecutive failure(s)", self._consecutive_failures
            )

    async def _sync_satellite_prefixes(self) -> tuple:
        # pylint: disable=too-many-locals
        """Sync all satellite prefixes from S3 to Redis. Returns (downloaded, errors)."""
        if self._client is None or self._redis_client is None:
            raise RuntimeError("S3 or Redis client is not initialized")

        sat_downloaded = 0
        errors = 0

        for prefix in self._sync_prefixes:
            channel_dir = self.PREFIX_TO_CHANNEL.get(
                prefix, prefix.rstrip("/").rsplit("/", maxsplit=1)[-1]
            )
            try:
                tileset_prefixes = await self._client.get_subdirectories(prefix)
                tileset_prefixes.sort()

                existing_tilesets = set(
                    await self._redis_client.get_satellite_tilesets(channel_dir)
                )

                prefix_downloaded = 0
                new_tilesets = 0
                for s3_tileset_prefix in tileset_prefixes:
                    tileset_id = s3_tileset_prefix.rstrip("/").split("/")[-1]

                    if tileset_id in existing_tilesets:
                        continue

                    new_tilesets += 1

                    downloaded = await self._client.sync_prefix_to_redis(
                        self._redis_client,
                        s3_tileset_prefix,
                        channel_dir,
                        tileset_id,
                        tile_ttl=self._settings.tile_ttl,
                    )
                    sat_downloaded += downloaded
                    prefix_downloaded += downloaded

                    score = self._extract_timestamp_score(tileset_id)
                    await self._redis_client.add_satellite_tileset(
                        channel_dir,
                        tileset_id,
                        score,
                        ttl=self._settings.tile_ttl,
                    )

                logger.info(
                    "[%s] %d in S3 | %d cached | %d new tilesets | %d tiles downloaded",
                    channel_dir,
                    len(tileset_prefixes),
                    len(existing_tilesets),
                    new_tilesets,
                    prefix_downloaded,
                )

            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to sync prefix '%s': %s", prefix, e)
                errors += 1

        return sat_downloaded, errors

    async def _sync_radar(self) -> tuple:
        """Sync radar tilesets from S3 to Redis. Returns (downloaded, errors)."""
        if self._client is None:
            raise RuntimeError("S3 client is not initialized")

        if self._redis_client is None:
            raise RuntimeError("Redis client is not initialized")

        total_downloaded = 0
        errors = 0
        new_tilesets = 0
        radar_ids_seen: set = set()

        try:
            # 1. List radar IDs: tiles/radar/{radar_id}/
            radar_prefixes = await self._client.get_subdirectories(RADAR_S3_PREFIX)

            for radar_prefix in radar_prefixes:
                radar_id = radar_prefix.rstrip("/").split("/")[-1]
                radar_ids_seen.add(radar_id)

                # 2. List variables: tiles/radar/{radar_id}/{variable_id}/
                var_prefixes = await self._client.get_subdirectories(radar_prefix)

                for var_prefix in var_prefixes:
                    variable_id = var_prefix.rstrip("/").split("/")[-1]

                    # 3. List elevations: tiles/radar/{radar_id}/{variable_id}/elev{N}/
                    elevation_prefixes = await self._client.get_subdirectories(
                        var_prefix
                    )

                    # Cache existing tilesets per elevation from Redis
                    existing_by_elevation: dict[str, set[str]] = {}

                    for elevation_prefix in elevation_prefixes:
                        elevation_id = elevation_prefix.rstrip("/").split("/")[-1]
                        if not elevation_id.startswith("elev"):
                            continue

                        total_downloaded += await self._sync_radar_elevation(
                            elevation_prefix,
                            radar_id,
                            variable_id,
                            elevation_id,
                            existing_by_elevation,
                        )

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Radar sync error: %s", e)
            errors += 1

        logger.info(
            "[radar] %d radar(s) scanned | %d new tilesets | %d tiles downloaded",
            len(radar_ids_seen),
            new_tilesets,
            total_downloaded,
        )
        return total_downloaded, errors

    async def _sync_radar_elevation(
        self,
        elevation_prefix: str,
        radar_id: str,
        variable_id: str,
        elevation_id: str,
        existing_by_elevation: dict[str, set[str]],
    ) -> int:
        """Sync all new tilesets under a single radar elevation prefix."""
        if self._client is None or self._redis_client is None:
            raise RuntimeError("S3 or Redis client is not initialized")

        if elevation_id not in existing_by_elevation:
            existing_by_elevation[elevation_id] = set(
                await self._redis_client.get_radar_tilesets(
                    radar_id, variable_id, elevation_id
                )
            )

        ts_prefixes = await self._client.get_subdirectories(elevation_prefix)
        downloaded_total = 0

        for ts_prefix in ts_prefixes:
            tileset_id = ts_prefix.rstrip("/").split("/")[-1]
            if tileset_id in existing_by_elevation[elevation_id]:
                continue

            downloaded = await self._client.sync_radar_prefix_to_redis(
                self._redis_client,
                ts_prefix,
                radar_id,
                variable_id,
                tileset_id,
                elevation_id,
                tile_ttl=self._settings.tile_ttl,
            )

            if downloaded > 0:
                await self._redis_client.add_radar_index(
                    radar_id,
                    variable_id,
                    elevation_id,
                    tileset_id,
                    ttl=self._settings.tile_ttl,
                )
                downloaded_total += downloaded
                logger.info(
                    "Radar sync: %d tiles for %s/%s/%s/%s",
                    downloaded,
                    radar_id,
                    variable_id,
                    elevation_id,
                    tileset_id,
                )

        return downloaded_total

    async def _count_radar_tilesets(self) -> int:
        """Count total radar tilesets from Redis index."""
        if self._redis_client is None:
            raise RuntimeError("Redis client is not initialized")

        count = 0
        radar_ids = await self._redis_client.get_radar_radars()
        for rid in radar_ids:
            variables = await self._redis_client.get_radar_variables(rid)
            for vid in variables:
                elevations = await self._redis_client.get_radar_elevations(rid, vid)
                for eid in elevations:
                    tilesets = await self._redis_client.get_radar_tilesets(
                        rid, vid, eid
                    )
                    count += len(tilesets)
        return count

    async def _sync_ecmwf_tp(self) -> tuple:
        """Sync ECMWF total precipitation forecasts/periods from S3 to Redis. Returns (downloaded, errors)."""
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
                    and is_centered_period_format(s.rstrip("/").split("/")[-1])
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

            logger.debug(
                "ECMWF-TP sync complete: %d forecasts active", len(active_forecasts)
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("ECMWF-TP sync error: %s", e)
            errors += 1

        return total_downloaded, errors

    async def _sync_ecmwf_mslp(self) -> tuple:
        """Sync ECMWF MSLP forecasts/timestamps from S3 to Redis. Returns (downloaded, errors)."""
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
                    b for b in basenames if is_centered_period_format(b)
                )

                known_timestamps = await self._redis_client.get_ecmwf_mslp_timestamps(
                    forecast_ts
                )
                new_timestamps = [t for t in timestamps if t not in known_timestamps]

                if new_timestamps:
                    stored = await self._client.sync_ecmwf_mslp_forecast_to_redis(
                        self._redis_client,
                        forecast_ts,
                        new_timestamps,
                        self._settings.ecmwf_mslp_geojson_ttl,
                    )
                    total_downloaded += stored
                    logger.info(
                        "ECMWF-MSLP synced %d geojsons for %s", stored, forecast_ts
                    )

                await self._redis_client.store_ecmwf_mslp_index(
                    forecast_ts, timestamps, self._settings.ecmwf_mslp_geojson_ttl
                )

            logger.debug(
                "ECMWF-MSLP sync complete: %d forecasts active",
                len(active_forecasts),
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("ECMWF-MSLP sync error: %s", e)
            errors += 1

        return total_downloaded, errors

    @staticmethod
    def _extract_timestamp_score(tileset_id: str) -> float:
        """Extract a numeric timestamp score from a tileset ID for sorted set ordering."""
        # Format: OR_ABI-L1b-RadF-M6C13_G19_s20250141230210...
        match = re.search(r"_s(\d{14})", tileset_id)
        if match:
            return float(match.group(1))
        # Fallback: use current time
        return time.time()


# Singleton instance for use across the application
sync_service = SyncService()
