"""
Background Sync Service.

Periodically syncs tile data from MinIO S3 bucket directly to Redis.
Runs as a background task during application lifetime.
"""

import asyncio
import logging
import fcntl
import re
import time
from typing import List, Optional

from clients.s3_client import S3Client
from clients.redis_client import RedisClient
from settings import Settings
from logging import Logger

logger = logging.getLogger(__name__)


class SyncService:
    """
    Background service that syncs tiles from S3 to Redis.

    Runs periodically to ensure Redis tile store stays in sync
    with the S3 bucket populated by tiles-processor.
    Uses Redis tileset index for incremental sync (only downloads
    new tilesets not already in Redis).
    """

    # Prefixes to sync from S3 (matches tiles-processor output structure)
    DEFAULT_SYNC_PREFIXES = [
        "band_13/tiles",
        "band_9/tiles",
        "band_2/tiles",
    ]

    # Maps S3 prefix to channel_dir for Redis key construction
    PREFIX_TO_CHANNEL = {
        "band_13/tiles": "band_13",
        "band_9/tiles": "band_9",
        "band_2/tiles": "band_2",
    }

    def __init__(
        self,
        settings: Optional[Settings] = None,
        sync_prefixes: Optional[List[str]] = None,
    ):
        self._settings = settings or Settings.get_settings()
        self._sync_prefixes = sync_prefixes or self.DEFAULT_SYNC_PREFIXES
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._client: Optional[S3Client] = None
        self._redis_client: Optional[RedisClient] = None
        self._lock_file_handle = None
        self._consecutive_failures = 0
        self._total_cycles = 0

    def set_redis_client(self, redis_client: RedisClient) -> None:
        """Set the Redis client (called during app startup)."""
        self._redis_client = redis_client

    def _create_client(self) -> S3Client:
        """Create S3 client from settings."""
        return S3Client(
            endpoint=self._settings.s3_tiles_data_endpoint,
            access_key=self._settings.s3_tiles_data_access_key,
            secret_key=self._settings.s3_tiles_data_secret_key,
            bucket=self._settings.s3_tiles_data_bucket_name,
            secure=self._settings.s3_tiles_data_secure,
        )

    async def start(self, logger: Logger) -> None:
        """Start the background sync task."""
        if self._running:
            logger.warning("Sync service is already running")
            return

        # Attempt to acquire lock
        try:
            self._lock_file_handle = open(self._settings.sync_lock_path, "w")
            fcntl.lockf(self._lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            logger.info("Sync service disabled (another worker is active).")
            if self._lock_file_handle:
                self._lock_file_handle.close()
                self._lock_file_handle = None
            return

        self._running = True
        self._task = asyncio.create_task(self._sync_loop())
        logger.info(
            f"Sync service started (Lock acquired). Interval: {self._settings.sync_interval_seconds}s, "
            f"Prefixes: {self._sync_prefixes}"
        )

    async def stop(self, logger: Logger) -> None:
        """Stop the background sync task."""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        # Release lock
        if self._lock_file_handle:
            try:
                fcntl.lockf(self._lock_file_handle, fcntl.LOCK_UN)
                self._lock_file_handle.close()
            except Exception as e:
                logger.error(f"Error releasing lock: {e}")
            self._lock_file_handle = None

        logger.info("Sync service stopped")

    async def _sync_loop(self) -> None:
        """Main sync loop with fixed-interval scheduling."""
        while self._running:
            cycle_start = time.monotonic()
            try:
                if not self._settings.is_s3_configured():
                    logger.warning(
                        "S3 not configured. "
                        "Set S3_TILES_DATA_ENDPOINT, S3_TILES_DATA_ACCESS_KEY, and S3_TILES_DATA_SECRET_KEY. "
                        "Retrying in next cycle..."
                    )
                else:
                    if not self._client:
                        self._client = self._create_client()

                    await self._run_sync()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
                self._consecutive_failures += 1

            # Fixed-interval scheduling: sleep for remaining time
            if self._running:
                elapsed = time.monotonic() - cycle_start
                sleep_time = max(0, self._settings.sync_interval_seconds - elapsed)
                await asyncio.sleep(sleep_time)

    async def _run_sync(self) -> None:
        """Execute a single sync cycle for all prefixes."""
        if not self._client or not self._redis_client:
            return

        sync_start = time.time()
        await self._redis_client.update_sync_status(
            {"is_running": "true", "last_sync_start": str(sync_start)}
        )

        logger.info("Starting sync cycle...")
        total_downloaded = 0
        errors = 0

        for prefix in self._sync_prefixes:
            channel_dir = self.PREFIX_TO_CHANNEL.get(prefix, prefix.split("/")[0])
            try:
                # 1. List S3 tileset prefixes
                tileset_prefixes = await self._client.get_subdirectories(prefix)
                tileset_prefixes.sort()

                # 2. Get tilesets already in Redis
                existing_tilesets = set(
                    await self._redis_client.get_satellite_tilesets(channel_dir)
                )

                # 3. Download only new tilesets
                for s3_tileset_prefix in tileset_prefixes:
                    # Extract tileset_id from prefix: "band_13/tiles/OR_ABI-.../"
                    tileset_dir = s3_tileset_prefix.rstrip("/").split("/")[-1]
                    # Remove "_tiles" suffix if present
                    tileset_id = tileset_dir.replace("_tiles", "")

                    if tileset_id in existing_tilesets:
                        continue

                    # Download and store in Redis
                    downloaded = await self._client.sync_prefix_to_redis(
                        self._redis_client,
                        s3_tileset_prefix,
                        channel_dir,
                        tileset_id,
                    )
                    total_downloaded += downloaded

                    # Add to tileset index with timestamp score
                    score = self._extract_timestamp_score(tileset_id)
                    await self._redis_client.add_satellite_tileset(
                        channel_dir, tileset_id, score
                    )

                # 4. Enforce retention: delete old tilesets from Redis first, then S3
                await self._enforce_retention_policy(
                    prefix, channel_dir, tileset_prefixes
                )

            except Exception as e:
                logger.error(f"Failed to sync prefix '{prefix}': {e}")
                errors += 1

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
            channel_dir = self.PREFIX_TO_CHANNEL.get(prefix, prefix.split("/")[0])
            tilesets = await self._redis_client.get_satellite_tilesets(channel_dir)
            sat_count += len(tilesets)

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
            }
        )

        if total_downloaded > 0:
            logger.info(
                f"Sync cycle completed: {total_downloaded} tiles downloaded "
                f"({duration_ms}ms)"
            )
        else:
            logger.info(f"Sync cycle completed: no new tiles ({duration_ms}ms)")

    async def _enforce_retention_policy(
        self,
        band_prefix: str,
        channel_dir: str,
        s3_tileset_prefixes: List[str],
    ) -> None:
        """
        Enforce retention policy: keep only the latest keep_count tilesets.
        Deletes from Redis first (safe), then from S3.
        """
        if not self._client or not self._redis_client:
            return

        keep_count = self._settings.keep_count

        try:
            # Get tilesets from Redis index (ordered by score, oldest first)
            redis_tilesets = await self._redis_client.get_satellite_tilesets(
                channel_dir
            )

            if len(redis_tilesets) <= keep_count:
                return

            # Tilesets to remove (oldest ones)
            tilesets_to_remove = redis_tilesets[:-keep_count]

            logger.info(
                f"Retention policy for {channel_dir}: "
                f"{len(redis_tilesets)} tilesets, keeping {keep_count}, "
                f"removing {len(tilesets_to_remove)}"
            )

            for tileset_id in tilesets_to_remove:
                # 1. Delete from Redis first (safe)
                await self._redis_client.delete_satellite_tileset(
                    channel_dir, tileset_id
                )

                # 2. Then delete from S3
                # Find the matching S3 prefix
                for s3_prefix in s3_tileset_prefixes:
                    if tileset_id in s3_prefix:
                        await self._client.delete_prefix(s3_prefix)
                        break

        except Exception as e:
            logger.error(f"Error enforcing retention for {channel_dir}: {e}")

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
