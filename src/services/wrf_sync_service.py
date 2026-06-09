"""Independent background sync loop for WRF tiles + GeoJSON overlays.

WRF is the heaviest product (thousands of tiles per forecast step), so it runs
on its own loop with a longer cadence and a generous watchdog — it can never
block satellite/radar/ECMWF, and they can never block it.
"""

import logging
from typing import List, Optional, Tuple

from clients.s3_client import S3Client
from services.domain_sync_service import DomainSyncService
from settings import Settings

logger = logging.getLogger(__name__)


class WrfSyncService(DomainSyncService):
    """Syncs WRF tilesets + overlays from S3 to Redis on its own loop."""

    def __init__(self, settings: Optional[Settings] = None):
        resolved = settings or Settings.get_settings()
        super().__init__(
            resolved,
            domain="wrf",
            lock_path=resolved.wrf_sync_lock_path,
            interval=resolved.wrf_sync_interval_seconds,
            timeout=resolved.wrf_sync_timeout_seconds,
            s3_concurrency=resolved.s3_max_concurrent_downloads,
            service_name="WRF sync",
        )

    async def _run_sync(self) -> None:
        await self._run_single_domain(self._sync_wrf())

    def _select_active_inits(self, init_tag_prefixes: List[str]) -> List[tuple]:
        """Pick the newest N init runs (newest-first) to sync this cycle.

        init_tag is ``YYYYMMDD_HHmmss`` (fixed-width) so lexicographic order
        equals chronological order. Bounds the per-cycle WRF walk as runs
        accumulate in S3. Returns ``[(init_tag, prefix), ...]``.
        """
        pairs = [(p.rstrip("/").split("/")[-1], p) for p in init_tag_prefixes]
        pairs.sort(key=lambda it: it[0], reverse=True)
        return pairs[: self._settings.wrf_inits_to_keep]

    async def _sync_wrf(self) -> Tuple[int, int]:
        # pylint: disable=too-many-nested-blocks
        """Sync WRF tilesets from S3 to Redis. Returns (downloaded, errors)."""
        if self._client is None or self._redis_client is None:
            raise RuntimeError("S3 or Redis client is not initialized")

        total_downloaded = 0
        errors = 0

        try:
            product_prefixes = await self._client.get_subdirectories(
                S3Client.WRF_TILES_PREFIX
            )

            for product_prefix in product_prefixes:
                product_id = product_prefix.rstrip("/").split("/")[-1]

                init_tag_prefixes = await self._client.get_subdirectories(
                    product_prefix
                )
                active_inits = self._select_active_inits(init_tag_prefixes)

                for init_tag, init_tag_prefix in active_inits:
                    fxxx_prefixes = await self._client.get_subdirectories(
                        init_tag_prefix
                    )

                    existing_steps = set(
                        await self._redis_client.get_wrf_steps(product_id, init_tag)
                    )

                    for fxxx_prefix in fxxx_prefixes:
                        fxxx = fxxx_prefix.rstrip("/").split("/")[-1]

                        if fxxx not in existing_steps:
                            downloaded = await self._client.sync_wrf_step_to_redis(
                                self._redis_client,
                                product_id,
                                init_tag,
                                fxxx,
                                tile_ttl=self._settings.wrf_tile_ttl,
                            )

                            if downloaded > 0:
                                init_score = self._extract_wrf_init_score(init_tag)
                                step_score = float(
                                    int(fxxx[1:]) if fxxx.startswith("F") else 0
                                )
                                await self._redis_client.add_wrf_index(
                                    product_id,
                                    init_tag,
                                    fxxx,
                                    init_score,
                                    step_score,
                                    ttl=self._settings.wrf_tile_ttl,
                                )
                                total_downloaded += downloaded
                                logger.info(
                                    "WRF sync: %d tiles for %s/%s/%s",
                                    downloaded,
                                    product_id,
                                    init_tag,
                                    fxxx,
                                )

                        # Always check overlays. Backend uploads tiles + GeoJSON
                        # in separate steps; an earlier sync cycle may have
                        # caught the tiles before the JSONs were uploaded, so
                        # `existing_steps` membership doesn't imply that the
                        # overlays were already mirrored to Redis. The overlay
                        # sync is idempotent (skips fast when Redis is up to
                        # date), so this is safe to call on every cycle.
                        await self._sync_wrf_overlays(product_id, init_tag, fxxx)

                # Reconcile the per-product init index to the active set so it
                # can't accumulate stale init runs over time. The init_runs
                # sorted set has its whole-key TTL refreshed on every new step,
                # so old members never expire on their own. Skip when the listing
                # is empty so a transient S3 error (get_subdirectories returns []
                # on failure) can't wipe the index — mirrors the ECMWF guard.
                if active_inits:
                    await self._redis_client.prune_wrf_inits(
                        product_id, [it for it, _ in active_inits]
                    )

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("WRF sync error: %s", e)
            errors += 1

        return total_downloaded, errors

    async def _sync_wrf_overlays(
        self, product_id: str, init_tag: str, fxxx: str
    ) -> None:
        """Sync the GeoJSON overlay layers (barbs / contours) for a step.

        Discovers the layer set on S3, registers it in the layers index, and
        copies each GeoJSON into Redis with the configured TTL. Failures are
        logged but do not interrupt the main tile-sync loop.
        """
        if self._client is None or self._redis_client is None:
            return
        # Skip the per-step S3 layer LIST entirely once a previous cycle
        # confirmed Redis mirrors every overlay for this step. The marker
        # self-expires with the overlay TTL, so steps are eventually rechecked.
        if await self._redis_client.is_wrf_overlays_complete(
            product_id, init_tag, fxxx
        ):
            return
        try:
            s3_layers = await self._client.list_wrf_layers(product_id, init_tag, fxxx)
            if not s3_layers:
                return

            # Idempotency guard: skip the per-layer S3 GET when Redis already
            # mirrors every layer reported by S3, and latch the completion
            # marker so later cycles skip the S3 LIST above too.
            redis_layers = set(
                await self._redis_client.get_wrf_layers(product_id, init_tag, fxxx)
            )
            missing_layers = [l for l in s3_layers if l not in redis_layers]
            if not missing_layers:
                await self._redis_client.set_wrf_overlays_complete(
                    product_id,
                    init_tag,
                    fxxx,
                    ttl=self._settings.wrf_geojson_ttl,
                )
                return

            await self._redis_client.add_wrf_layers(
                product_id,
                init_tag,
                fxxx,
                s3_layers,
                ttl=self._settings.wrf_geojson_ttl,
            )
            for layer in missing_layers:
                await self._client.sync_wrf_geojson_to_redis(
                    self._redis_client,
                    product_id,
                    init_tag,
                    fxxx,
                    layer,
                    geojson_ttl=self._settings.wrf_geojson_ttl,
                )
            logger.info(
                "WRF sync: %d GeoJSON layers for %s/%s/%s",
                len(missing_layers),
                product_id,
                init_tag,
                fxxx,
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(
                "WRF GeoJSON sync error for %s/%s/%s: %s",
                product_id,
                init_tag,
                fxxx,
                e,
            )

    @staticmethod
    def _extract_wrf_init_score(init_tag: str) -> float:
        """Extract a numeric score from init_tag for sorted set ordering.

        Format: 20260430_060000 → 20260430060000.0
        """
        return float(init_tag.replace("_", ""))


# Singleton instance for use across the application
wrf_sync_service = WrfSyncService()
