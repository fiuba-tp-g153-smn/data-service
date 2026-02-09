"""
Background Radar Sync Service.

Periodically scans the output_radar/ volume mount, reads tile files,
and stores them in Redis with TTL. This replaces direct filesystem
access from the request path.
"""

import asyncio
import fcntl
import logging
import time
from logging import Logger
from pathlib import Path
from typing import Optional, Set

from clients.redis_client import RedisClient
from settings import Settings

logger = logging.getLogger(__name__)


class RadarSyncService:
    """
    Background service that preloads radar tiles from the filesystem into Redis.

    Scans the output_radar/ directory periodically and loads new tilesets
    into Redis with a 1-hour TTL. Tracks loaded tilesets to avoid re-loading.
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        radar_path: Optional[Path] = None,
    ):
        self._settings = settings or Settings.get_settings()
        self._radar_path = radar_path or Path.cwd().parent / "output_radar"
        self._redis_client: Optional[RedisClient] = None
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._lock_file_handle = None
        self._loaded_tilesets: Set[str] = set()

    def set_redis_client(self, redis_client: RedisClient) -> None:
        """Set the Redis client (called during app startup)."""
        self._redis_client = redis_client

    async def start(self, app_logger: Logger) -> None:
        """Start the background radar sync task."""
        if self._running:
            app_logger.warning("Radar sync service is already running")
            return

        if not self._radar_path.exists():
            logger.info(
                f"Radar path {self._radar_path} does not exist. " "Radar sync disabled."
            )
            return

        # Attempt to acquire lock
        try:
            self._lock_file_handle = open(
                self._settings.radar_lock_path, "w", encoding="utf-8"
            )
            fcntl.lockf(self._lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            app_logger.info("Radar sync service disabled (another worker is active).")
            if self._lock_file_handle:
                self._lock_file_handle.close()
                self._lock_file_handle = None
            return

        self._running = True
        self._task = asyncio.create_task(self._sync_loop())
        app_logger.info(
            "Radar sync service started. Interval: %ss, Path: %s",
            self._settings.radar_sync_interval_seconds,
            self._radar_path,
        )

    async def stop(self, app_logger: Logger) -> None:
        """Stop the background radar sync task."""
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
            except Exception as e:  # pylint: disable=broad-exception-caught
                app_logger.error("Error releasing radar lock: %s", e)
            self._lock_file_handle = None

        app_logger.info("Radar sync service stopped")

    async def _sync_loop(self) -> None:
        """Main sync loop with fixed-interval scheduling."""
        while self._running:
            cycle_start = time.monotonic()
            try:
                await self._run_sync()
            except asyncio.CancelledError:
                break
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Error in radar sync loop: %s", e)

            if self._running:
                elapsed = time.monotonic() - cycle_start
                sleep_time = max(
                    0,
                    self._settings.radar_sync_interval_seconds - elapsed,
                )
                await asyncio.sleep(sleep_time)

    async def _run_sync(self) -> None:
        # pylint: disable=too-many-locals
        """Execute a single radar sync cycle."""
        if not self._redis_client:
            return

        if not await asyncio.to_thread(self._radar_path.exists):
            return

        radar_dirs = await asyncio.to_thread(self._list_dirs, self._radar_path)
        radar_count = 0

        for radar_dir in radar_dirs:
            radar_id = radar_dir.name
            variable_dirs = await asyncio.to_thread(self._list_dirs, radar_dir)

            for var_dir in variable_dirs:
                variable_id = var_dir.name
                tileset_dirs = await asyncio.to_thread(self._list_dirs, var_dir)

                for ts_dir in tileset_dirs:
                    # Folder name: {TIMESTAMP}_elev{N}
                    parts = ts_dir.name.split("_elev")
                    if len(parts) != 2:
                        continue

                    tileset_id = parts[0]
                    elevation_id = f"elev{parts[1]}"

                    # Unique key for tracking loaded tilesets
                    ts_key = f"{radar_id}/{variable_id}/{tileset_id}/{elevation_id}"

                    if ts_key in self._loaded_tilesets:
                        continue

                    # Load tiles from this tileset directory
                    tiles_dir = ts_dir / "tiles"
                    if not await asyncio.to_thread(tiles_dir.exists):
                        continue

                    loaded = await self._load_tileset(
                        tiles_dir,
                        radar_id,
                        variable_id,
                        tileset_id,
                        elevation_id,
                    )

                    if loaded > 0:
                        # Add to radar index
                        await self._redis_client.add_radar_index(
                            radar_id,
                            variable_id,
                            elevation_id,
                            tileset_id,
                            ttl=self._settings.tile_ttl,
                        )
                        self._loaded_tilesets.add(ts_key)
                        radar_count += loaded

        # Update sync status with radar count
        if radar_count > 0:
            logger.info("Radar sync: loaded %d new tiles", radar_count)

        # Update radar tilesets count in sync status
        await self._redis_client.update_sync_status(
            {"radar_tilesets_count": str(len(self._loaded_tilesets))}
        )

    async def _load_tileset(
        self,
        tiles_dir: Path,
        radar_id: str,
        variable_id: str,
        tileset_id: str,
        elevation_id: str,
    ) -> int:
        """Load all tiles from a tileset directory into Redis."""
        tile_files = await asyncio.to_thread(self._find_webp_files, tiles_dir)

        loaded = 0
        for tile_file in tile_files:
            try:
                # Parse z/x/y from path: tiles/{z}/{x}/{y}.webp
                rel = tile_file.relative_to(tiles_dir)
                parts = rel.parts
                if len(parts) != 3:
                    continue

                z = int(parts[0])
                x = int(parts[1])
                y = int(parts[2].replace(".webp", ""))

                content = await asyncio.to_thread(tile_file.read_bytes)
                await self._redis_client.store_radar_tile(
                    radar_id,
                    variable_id,
                    tileset_id,
                    elevation_id,
                    z,
                    x,
                    y,
                    content,
                    ttl=self._settings.tile_ttl,
                )
                loaded += 1
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to load radar tile %s: %s", tile_file, e)

        return loaded

    @staticmethod
    def _list_dirs(path: Path) -> list:
        """List subdirectories of a path (blocking)."""
        if not path.exists():
            return []
        return [d for d in path.iterdir() if d.is_dir()]

    @staticmethod
    def _find_webp_files(path: Path) -> list:
        """Find all .webp files under a path (blocking)."""
        if not path.exists():
            return []
        return list(path.rglob("*.webp"))


# Singleton instance
radar_sync_service = RadarSyncService()
