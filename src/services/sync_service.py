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
from settings import Settings

logger = logging.getLogger(__name__)

# S3 prefix where radar data lives
RADAR_S3_PREFIX = "radar"


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
        "band_13/tiles",
        "band_9/tiles",
        "band_2/tiles",
        "glm_fed/tiles",
        "glm_toe/tiles",
        "glm_mfa/tiles",
    ]

    # Maps S3 prefix to channel_dir for Redis key construction
    PREFIX_TO_CHANNEL = {
        "band_13/tiles": "band_13",
        "band_9/tiles": "band_9",
        "band_2/tiles": "band_2",
        "glm_fed/tiles": "glm_fed",
        "glm_toe/tiles": "glm_toe",
        "glm_mfa/tiles": "glm_mfa",
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
        # pylint: disable=too-many-locals
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

        logger.info("Starting sync cycle...")
        total_downloaded = 0
        errors = 0

        for prefix in self._sync_prefixes:
            channel_dir = self.PREFIX_TO_CHANNEL.get(
                prefix, prefix.split("/", maxsplit=1)[0]
            )
            try:
                # 1. List S3 tileset prefixes
                tileset_prefixes = await self._client.get_subdirectories(prefix)
                tileset_prefixes.sort()

                # 2. Get tilesets already in Redis
                existing_tilesets = set(
                    await self._redis_client.get_satellite_tilesets(channel_dir)
                )

                # 3. Download only new tilesets (with TTL for automatic eviction)
                for s3_tileset_prefix in tileset_prefixes:
                    # Extract tileset_id from prefix: "band_13/tiles/20260521320209/"
                    tileset_id = s3_tileset_prefix.rstrip("/").split("/")[-1]

                    if tileset_id in existing_tilesets:
                        continue

                    # Download and store in Redis with TTL
                    downloaded = await self._client.sync_prefix_to_redis(
                        self._redis_client,
                        s3_tileset_prefix,
                        channel_dir,
                        tileset_id,
                        tile_ttl=self._settings.tile_ttl,
                    )
                    total_downloaded += downloaded

                    # Add to tileset index with timestamp score and TTL
                    score = self._extract_timestamp_score(tileset_id)
                    await self._redis_client.add_satellite_tileset(
                        channel_dir,
                        tileset_id,
                        score,
                        ttl=self._settings.tile_ttl,
                    )

            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to sync prefix '%s': %s", prefix, e)
                errors += 1

        # ── Radar sync ──
        radar_downloaded, radar_errors = await self._sync_radar()
        total_downloaded += radar_downloaded
        errors += radar_errors

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
                prefix, prefix.split("/", maxsplit=1)[0]
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

        if total_downloaded > 0:
            logger.info(
                "Sync cycle completed: %d tiles downloaded (%dms)",
                total_downloaded,
                duration_ms,
            )
        else:
            logger.info("Sync cycle completed: no new tiles (%dms)", duration_ms)

    async def _sync_radar(self) -> tuple:
        """Sync radar tilesets from S3 to Redis. Returns (downloaded, errors)."""
        if self._client is None:
            raise RuntimeError("S3 client is not initialized")

        if self._redis_client is None:
            raise RuntimeError("Redis client is not initialized")

        total_downloaded = 0
        errors = 0

        try:
            # 1. List radar IDs: radar/{radar_id}/
            radar_prefixes = await self._client.get_subdirectories(RADAR_S3_PREFIX)

            for radar_prefix in radar_prefixes:
                radar_id = radar_prefix.rstrip("/").split("/")[-1]

                # 2. List variables: radar/{radar_id}/{variable_id}/
                var_prefixes = await self._client.get_subdirectories(radar_prefix)

                for var_prefix in var_prefixes:
                    variable_id = var_prefix.rstrip("/").split("/")[-1]

                    # 3. List tileset dirs: radar/{radar_id}/{variable_id}/{ts}_elev{N}/
                    ts_prefixes = await self._client.get_subdirectories(var_prefix)

                    # Cache existing tilesets per elevation from Redis
                    existing_by_elevation = {}

                    for ts_prefix in ts_prefixes:
                        folder_name = ts_prefix.rstrip("/").split("/")[-1]
                        parts = folder_name.split("_elev")
                        if len(parts) != 2:
                            continue

                        tileset_id = parts[0]
                        elevation_id = f"elev{parts[1]}"

                        # Query Redis once per elevation (same pattern as satellite)
                        if elevation_id not in existing_by_elevation:
                            existing_by_elevation[elevation_id] = set(
                                await self._redis_client.get_radar_tilesets(
                                    radar_id, variable_id, elevation_id
                                )
                            )

                        if tileset_id in existing_by_elevation[elevation_id]:
                            continue

                        # 4. Download tiles under tileset prefix
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
                            total_downloaded += downloaded
                            logger.info(
                                "Radar sync: %d tiles for %s/%s/%s/%s",
                                downloaded,
                                radar_id,
                                variable_id,
                                tileset_id,
                                elevation_id,
                            )

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Radar sync error: %s", e)
            errors += 1

        return total_downloaded, errors

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
