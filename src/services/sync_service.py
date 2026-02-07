"""
Background Sync Service.

Periodically syncs tile data from MinIO S3 bucket to local storage.
Runs as a background task during application lifetime.
"""

import asyncio
import logging
import fcntl
import os
from pathlib import Path
from typing import List, Optional

from clients.s3_client import S3Client
from settings import Settings
from logging import Logger

logger = logging.getLogger(__name__)


class SyncService:
    """
    Background service that syncs tiles from S3 to local storage.

    Runs periodically (default: every 60 seconds) to ensure local tile
    storage stays in sync with the S3 bucket populated by tiles-processor.

    Attributes:
        _settings: Application settings with S3 configuration
        _client: S3 client

        _sync_prefixes: List of S3 prefixes to sync
        _local_base_path: Local base directory for synced files
        _task: Background asyncio task
        _running: Flag indicating if sync is active
    """

    # Prefixes to sync from S3 (matches tiles-processor output structure)
    DEFAULT_SYNC_PREFIXES = [
        "band_13/tiles",
        "band_9/tiles",
        "band_2/tiles",
    ]

    def __init__(
        self,
        settings: Optional[Settings] = None,
        local_base_path: Optional[Path] = None,
        sync_prefixes: Optional[List[str]] = None,
    ):
        self._settings = settings or Settings.get_settings()
        self._local_base_path = local_base_path or Path.cwd() / "data/tmp"
        self._sync_prefixes = sync_prefixes or self.DEFAULT_SYNC_PREFIXES
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._client: Optional[S3Client] = None

        # Lock file mechanism to ensure only one worker syncs
        self._lock_file_path = "/tmp/data-service-sync.lock"
        self._lock_file_handle = None

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
            self._lock_file_handle = open(self._lock_file_path, "w")
            fcntl.lockf(self._lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            # Lock is held by another process
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
        """Main sync loop that runs periodically."""
        while self._running:
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

                    # Execute sync cycle
                    await self._run_sync()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")

            # Wait for next interval
            if self._running:
                await asyncio.sleep(self._settings.sync_interval_seconds)

    async def _run_sync(self) -> None:
        """Execute a single sync cycle for all prefixes."""
        if not self._client:
            return

        logger.info("Starting sync cycle...")
        total_downloaded = 0

        for prefix in self._sync_prefixes:
            try:
                # 1. Enforce retention policy before sync
                await self._enforce_retention_policy(prefix)

                # 2. Map S3 prefix to local directory
                # e.g., "band_13/tiles" -> local_base_path/band_13/tiles
                local_dir = self._local_base_path / prefix

                downloaded = await self._client.sync_prefix(
                    s3_prefix=prefix,
                    local_dir=local_dir,
                    delete_orphans=True,
                )
                total_downloaded += downloaded
            except Exception as e:
                logger.error(f"Failed to sync prefix '{prefix}': {e}")

        if total_downloaded > 0:
            logger.info(f"Sync cycle completed: {total_downloaded} files downloaded")
        else:
            logger.info("Sync cycle completed: no new files")

    async def _enforce_retention_policy(self, band_prefix: str) -> None:
        """
        Enforce retention policy: keep only the latest 26 tilesets (prefixes).
        Oldest prefixes are deleted from S3.
        """
        if not self._client:
            return

        KEEP_COUNT = 26

        try:
            # 1. List tileset prefixes (subdirectories)
            # keys look like: band_13/tiles/OR_ABI-L1b-RadF-M6C13_G19_s20250141230210.../
            tileset_prefixes = await self._client.get_subdirectories(band_prefix)

            # 2. Sort lexicographically (effectively chronological due to filename format: sYYYYJJJHHMMSSS)
            tileset_prefixes.sort()

            # 3. Check threshold
            if len(tileset_prefixes) <= KEEP_COUNT:
                return

            # 4. Prune excess
            # We want to keep the last KEEP_COUNT items.
            # Delete items from index 0 to (len - KEEP_COUNT)
            prefixes_to_delete = tileset_prefixes[:-KEEP_COUNT]

            if prefixes_to_delete:
                logger.info(
                    f"Retention policy triggered for {band_prefix}. "
                    f"Found {len(tileset_prefixes)} tilesets, limit is {KEEP_COUNT}. "
                    f"Deleting {len(prefixes_to_delete)} old tilesets."
                )

                for prefix_to_delete in prefixes_to_delete:
                    await self._client.delete_prefix(prefix_to_delete)

        except Exception as e:
            logger.error(f"Error enforcing retention policy for {band_prefix}: {e}")


# Singleton instance for use across the application
sync_service = SyncService()
