"""Independent background sync loop for satellite tiles (GOES-19 ABI + GLM)."""

import logging
import time
from typing import List, Optional, Tuple

from services.domain_sync_service import DomainSyncService
from settings import Settings

logger = logging.getLogger(__name__)


class SatelliteSyncService(DomainSyncService):
    """Syncs satellite band/GLM tilesets from S3 to Redis on its own loop."""

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
        resolved = settings or Settings.get_settings()
        self._sync_prefixes = sync_prefixes or self.DEFAULT_SYNC_PREFIXES
        super().__init__(
            resolved,
            domain="satellite",
            lock_path=resolved.sync_lock_path,
            interval=resolved.sync_interval_seconds,
            timeout=resolved.sync_domain_timeout_seconds,
            s3_concurrency=resolved.s3_max_concurrent_downloads,
            service_name="Satellite sync",
        )

    async def _run_sync(self) -> None:
        await self._run_single_domain(self._sync_satellite_prefixes())

    async def _sync_satellite_prefixes(self) -> Tuple[int, int]:
        # pylint: disable=too-many-locals
        """Sync all satellite prefixes from S3 to Redis. Returns (downloaded, errors)."""
        if self._client is None or self._redis_client is None:
            raise RuntimeError("S3 or Redis client is not initialized")

        sat_downloaded = 0
        errors = 0

        now = time.time()
        cutoff = now - self._settings.satellite_tile_ttl

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
                        tile_ttl=self._settings.satellite_tile_ttl,
                    )
                    sat_downloaded += downloaded
                    prefix_downloaded += downloaded

                    if downloaded > 0:
                        # Only index a tileset whose tiles actually landed in
                        # Redis, so a transient empty/failed S3 read isn't cached
                        # as "present" and then skipped until the next trim
                        # (~tile_ttl). Score = insertion time, matching the
                        # per-tile Redis TTL so index entries expire in lockstep
                        # with the tiles they point to.
                        await self._redis_client.add_satellite_tileset(
                            channel_dir,
                            tileset_id,
                            now,
                            ttl=self._settings.satellite_tile_ttl,
                        )

                # Trim every cycle (even with no new tilesets) so the index stays
                # bounded to the live-tile window instead of growing unboundedly.
                trimmed = await self._redis_client.trim_satellite_index(
                    channel_dir, cutoff
                )

                logger.info(
                    "[%s] %d in S3 | %d cached | %d new tilesets"
                    " | %d tiles downloaded | %d expired trimmed",
                    channel_dir,
                    len(tileset_prefixes),
                    len(existing_tilesets),
                    new_tilesets,
                    prefix_downloaded,
                    trimmed,
                )

            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to sync prefix '%s': %s", prefix, e)
                errors += 1

        return sat_downloaded, errors


# Singleton instance for use across the application
satellite_sync_service = SatelliteSyncService()
