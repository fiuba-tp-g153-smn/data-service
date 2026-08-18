"""Independent background sync loop for radar tiles."""

import logging
import time
from typing import Optional, Tuple

from services.domain_sync_service import DomainSyncService
from settings import Settings

logger = logging.getLogger(__name__)

# S3 prefix where radar data lives
RADAR_S3_PREFIX = "tiles/radar"


class RadarSyncService(DomainSyncService):
    """Syncs radar tilesets from S3 to Redis on its own loop."""

    def __init__(self, settings: Optional[Settings] = None):
        resolved = settings or Settings.get_settings()
        super().__init__(
            resolved,
            domain="radar",
            lock_path=resolved.radar_lock_path,
            interval=resolved.sync_interval_seconds,
            timeout=resolved.sync_domain_timeout_seconds,
            s3_concurrency=resolved.s3_max_concurrent_downloads,
            service_name="Radar sync",
        )

    async def _run_sync(self) -> None:
        await self._run_single_domain(self._sync_radar())

    async def _sync_radar(self) -> Tuple[int, int]:
        """Sync radar tilesets from S3 to Redis. Returns (downloaded, errors)."""
        if self._client is None:
            raise RuntimeError("S3 client is not initialized")

        if self._redis_client is None:
            raise RuntimeError("Redis client is not initialized")

        total_downloaded = 0
        errors = 0
        radar_ids_seen: set = set()

        now = time.time()
        cutoff = now - self._settings.radar_tile_ttl

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
                            now,
                            cutoff,
                        )

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Radar sync error: %s", e)
            errors += 1

        logger.info(
            "[radar] %d radar(s) scanned | %d tiles downloaded",
            len(radar_ids_seen),
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
        now: float,
        cutoff: float,
    ) -> int:
        # pylint: disable=too-many-arguments
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
                tile_ttl=self._settings.radar_tile_ttl,
            )

            if downloaded > 0:
                # Score = insertion time, matching the per-tile Redis TTL.
                await self._redis_client.add_radar_index(
                    radar_id,
                    variable_id,
                    elevation_id,
                    tileset_id,
                    now,
                    ttl=self._settings.radar_tile_ttl,
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

        # Trim every cycle so the index stays bounded to the live-tile window.
        await self._redis_client.trim_radar_index(
            radar_id, variable_id, elevation_id, cutoff
        )

        return downloaded_total


# Singleton instance for use across the application
radar_sync_service = RadarSyncService()
