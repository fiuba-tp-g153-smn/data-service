"""
Background Radar Sync Service.

Periodically syncs radar tile data from MinIO S3 bucket to Redis.
S3 structure: radar/{radar_id}/{variable_id}/{timestamp}_elev{N}/{z}/{x}/{y}.webp
"""

import logging
import re
import time
from logging import Logger
from typing import List, Optional, Set

from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.base_sync_service import BaseSyncService
from settings import Settings

logger = logging.getLogger(__name__)

# S3 prefix where radar data lives
RADAR_S3_PREFIX = "radar"


class RadarSyncService(BaseSyncService):
    """
    Background service that syncs radar tiles from S3 to Redis.

    Lists radar prefixes in S3 periodically and downloads new tilesets
    into Redis with TTL. Mirrors the satellite SyncService pattern.
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
    ):
        resolved_settings = settings or Settings.get_settings()
        super().__init__(
            settings=resolved_settings,
            sync_interval=resolved_settings.radar_sync_interval_seconds,
            service_name="Radar sync service",
        )
        self._client: Optional[S3Client] = None
        self._redis_client: Optional[RedisClient] = None
        self._loaded_tilesets: Set[str] = set()

    def set_redis_client(self, redis_client: RedisClient) -> None:
        """Set the Redis client (called during app startup)."""
        self._redis_client = redis_client

    def _get_lock_path(self) -> str:
        """Return the radar sync lock file path."""
        return self._settings.radar_lock_path

    def _create_client(self) -> S3Client:
        """Create S3 client from settings."""
        return S3Client(
            endpoint=self._settings.s3_tiles_data_endpoint,
            access_key=self._settings.s3_tiles_data_access_key,
            secret_key=self._settings.s3_tiles_data_secret_key,
            bucket=self._settings.s3_tiles_data_bucket_name,
            secure=self._settings.s3_tiles_data_secure,
        )

    def _log_started(self, app_logger: Logger) -> None:
        """Log radar-specific start message."""
        app_logger.info(
            "Radar sync service started. Interval: %ss, S3 prefix: %s/",
            self._sync_interval,
            RADAR_S3_PREFIX,
        )

    def _on_sync_error(self, error: Exception) -> None:
        """Track sync loop errors."""
        logger.error("Radar sync error: %s", error)

    async def _run_sync(self) -> None:
        # pylint: disable=too-many-locals
        """Execute a single radar sync cycle from S3."""
        if not self._settings.is_s3_configured():
            logger.warning("S3 not configured. Radar sync skipped.")
            return

        if not self._client:
            self._client = self._create_client()

        if not self._redis_client:
            return

        total_downloaded = 0

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

                for ts_prefix in ts_prefixes:
                    folder_name = ts_prefix.rstrip("/").split("/")[-1]
                    parts = folder_name.split("_elev")
                    if len(parts) != 2:
                        continue

                    tileset_id = parts[0]
                    elevation_id = f"elev{parts[1]}"

                    ts_key = (
                        f"{radar_id}/{variable_id}/{tileset_id}/{elevation_id}"
                    )
                    if ts_key in self._loaded_tilesets:
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
                        self._loaded_tilesets.add(ts_key)
                        total_downloaded += downloaded

        if total_downloaded > 0:
            logger.info("Radar sync: downloaded %d new tiles", total_downloaded)

        await self._redis_client.update_sync_status(
            {"radar_tilesets_count": str(len(self._loaded_tilesets))}
        )


# Singleton instance
radar_sync_service = RadarSyncService()
