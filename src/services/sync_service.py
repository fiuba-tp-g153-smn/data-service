"""
Background Sync Service.

Periodically syncs tile data from MinIO S3 bucket to local storage.
Runs as a background task during application lifetime.
"""

import asyncio
import logging
from pathlib import Path
from typing import List, Optional

from clients.minio_sync_client import MinioSyncClient
from settings import Settings
from logging import Logger
logger = logging.getLogger(__name__)


class SyncService:
    """
    Background service that syncs tiles from MinIO to local storage.

    Runs periodically (default: every 60 seconds) to ensure local tile
    storage stays in sync with the MinIO bucket populated by tiles-processor.

    Attributes:
        _settings: Application settings with MinIO configuration
        _client: MinIO sync client
        _sync_prefixes: List of S3 prefixes to sync
        _local_base_path: Local base directory for synced files
        _task: Background asyncio task
        _running: Flag indicating if sync is active
    """

    # Prefixes to sync from MinIO (matches tiles-processor output structure)
    DEFAULT_SYNC_PREFIXES = [
        "band_13/tiles",
        "band_9/tiles",
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
        self._client: Optional[MinioSyncClient] = None

    def _create_client(self) -> MinioSyncClient:
        """Create MinIO sync client from settings."""
        return MinioSyncClient(
            endpoint=self._settings.minio_endpoint,
            access_key=self._settings.minio_access_key,
            secret_key=self._settings.minio_secret_key,
            bucket=self._settings.minio_bucket,
            secure=self._settings.minio_secure,
        )

    async def start(self, logger: Logger) -> None:
        """Start the background sync task."""
        if not self._settings.is_minio_configured():
            logger.warning(
                "MinIO not configured. Sync service will not start. "
                "Set MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY."
            )
            return

        if self._running:
            logger.warning("Sync service is already running")
            return

        self._client = self._create_client()

        # Check connection before starting
        if not await self._client.check_connection():
            logger.error(
                "Failed to connect to MinIO. Sync service will not start. "
                f"Endpoint: {self._settings.minio_endpoint}, "
                f"Bucket: {self._settings.minio_bucket}"
            )
            return

        self._running = True
        self._task = asyncio.create_task(self._sync_loop())
        logger.info(
            f"Sync service started. Interval: {self._settings.sync_interval_seconds}s, "
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

        logger.info("Sync service stopped")

    async def _sync_loop(self) -> None:
        """Main sync loop that runs periodically."""
        # Run initial sync immediately
        await self._run_sync()

        while self._running:
            try:
                await asyncio.sleep(self._settings.sync_interval_seconds)
                if self._running:
                    await self._run_sync()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
                # Continue running despite errors
                await asyncio.sleep(self._settings.sync_interval_seconds)

    async def _run_sync(self) -> None:
        """Execute a single sync cycle for all prefixes."""
        if not self._client:
            return

        logger.info("Starting sync cycle...")
        total_downloaded = 0

        for prefix in self._sync_prefixes:
            try:
                # Map S3 prefix to local directory
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


# Singleton instance for use across the application
sync_service = SyncService()
