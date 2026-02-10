"""
Background Radar Sync Service.

Periodically scans the output_radar/ volume mount, reads tile files,
and stores them in Redis with TTL. This replaces direct filesystem
access from the request path.
"""

import asyncio
import logging
from logging import Logger
from pathlib import Path
from typing import Optional, Set

from clients.redis_client import RedisClient
from services.base_sync_service import BaseSyncService
from settings import Settings

logger = logging.getLogger(__name__)


class RadarSyncService(BaseSyncService):
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
        resolved_settings = settings or Settings.get_settings()
        self._radar_path = radar_path or Path.cwd().parent / "output_radar"
        super().__init__(
            settings=resolved_settings,
            sync_interval=resolved_settings.radar_sync_interval_seconds,
            service_name="Radar sync service",
        )
        self._redis_client: Optional[RedisClient] = None
        self._loaded_tilesets: Set[str] = set()

    def set_redis_client(self, redis_client: RedisClient) -> None:
        """Set the Redis client (called during app startup)."""
        self._redis_client = redis_client

    def _get_lock_path(self) -> str:
        """Return the radar sync lock file path."""
        return self._settings.radar_lock_path

    def _pre_start_check(self, app_logger: Logger) -> bool:
        """Check that the radar output directory exists before starting."""
        if not self._radar_path.exists():
            logger.info(
                "Radar path %s does not exist. Radar sync disabled.",
                self._radar_path,
            )
            return False
        return True

    def _log_started(self, app_logger: Logger) -> None:
        """Log radar-specific start message."""
        app_logger.info(
            "Radar sync service started. Interval: %ss, Path: %s",
            self._sync_interval,
            self._radar_path,
        )

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

    async def _load_tileset(  # pylint: disable=too-many-positional-arguments
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
