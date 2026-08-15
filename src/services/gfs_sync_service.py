"""Independent background sync loop for GFS listings and overlays.

Deliberately narrower than the other model loops: it mirrors **only** the
cycle/step indexes and the single-file GeoJSON overlays. Raster tiles and barb
tiles are left out — one cycle is ~75k tiles across 500/250 hPa, so they are
read on demand from S3 and cached lazily by `GfsOnDemandStrategy`.

That makes a pass cheap (a handful of LISTs plus a few small JSONs per step),
which is what lets it run on a short interval while a cycle fills in gradually.
"""

import logging
import time
from typing import List, Optional, Tuple

from clients.s3_client import S3Client
from services.domain_sync_service import DomainSyncService
from services.gfs_config import (
    GFS_PRODUCTS,
    GfsProduct,
    leaf_segment,
    step_from_basename,
)
from settings import Settings

logger = logging.getLogger(__name__)


class GfsSyncService(DomainSyncService):
    """Mirrors GFS listings + overlays from S3 to Redis on its own loop."""

    def __init__(self, settings: Optional[Settings] = None):
        resolved = settings or Settings.get_settings()
        super().__init__(
            resolved,
            domain="gfs",
            lock_path=resolved.gfs_sync_lock_path,
            interval=resolved.gfs_sync_interval_seconds,
            timeout=resolved.gfs_sync_timeout_seconds,
            s3_concurrency=resolved.s3_max_concurrent_downloads,
            service_name="GFS sync",
        )

    async def _run_sync(self) -> None:
        await self._run_single_domain(self._sync_gfs())

    async def _sync_gfs(self) -> Tuple[int, int]:
        """Mirror every active cycle of every product. Returns (synced, errors)."""
        if self._client is None or self._redis_client is None:
            raise RuntimeError("S3 or Redis client is not initialized")

        total = 0
        errors = 0
        scan_start = time.monotonic()

        for product in GFS_PRODUCTS.values():
            try:
                total += await self._sync_product(product)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error("GFS sync error for %s: %s", product.product_id, exc)
                errors += 1

        logger.info(
            "[gfs] %d product(s) scanned | %d overlay(s) mirrored | %.1fs",
            len(GFS_PRODUCTS),
            total,
            time.monotonic() - scan_start,
        )
        return total, errors

    async def _sync_product(self, product: GfsProduct) -> int:
        """Mirror the newest cycles of one product. Returns overlays copied."""
        assert self._client is not None and self._redis_client is not None

        cycles = await self._active_cycles(product)
        if not cycles:
            return 0

        copied = 0
        for cycle in cycles:
            for fxxx in await self._list_steps(product, cycle):
                copied += await self._sync_step(product, cycle, fxxx)

        # Reconcile the cycle index to the active set. The sorted set has its
        # whole-key TTL refreshed on every new step, so retired cycles would
        # never expire on their own. Guarded on a non-empty listing so a
        # transient S3 error (which yields []) cannot wipe the index — same
        # guard ECMWF and WRF use.
        await self._redis_client.prune_gfs_cycles(product.product_id, cycles)
        return copied

    async def _active_cycles(self, product: GfsProduct) -> List[str]:
        """Newest `gfs_cycles_to_keep` cycles of a product, newest first.

        Discovered from the COG prefix rather than the tiles prefix: `mslp`
        produces no raster, so listing tiles would make it look like it has no
        cycles at all.
        """
        assert self._client is not None
        prefixes = await self._client.get_subdirectories(
            S3Client.gfs_cog_cycle_prefix(product.s3_segment)
        )
        # Cycle tags are fixed-width `YYYYMMDDTHHmmZ`, so lexicographic order
        # equals chronological order.
        cycles = sorted(
            (name for name in (leaf_segment(p) for p in prefixes) if name), reverse=True
        )
        return cycles[: self._settings.gfs_cycles_to_keep]

    async def _list_steps(self, product: GfsProduct, cycle: str) -> List[str]:
        """Steps of a cycle, recovered from the COG basenames."""
        assert self._client is not None
        basenames = await self._client.list_object_basenames(
            f"{S3Client.gfs_cog_cycle_prefix(product.s3_segment)}{cycle}/",
            ".tif",
            delimiter="/",
        )
        return sorted(
            step
            for step in (step_from_basename(name, cycle) for name in basenames)
            if step
        )

    async def _sync_step(self, product: GfsProduct, cycle: str, fxxx: str) -> int:
        """Index one step and mirror any overlay Redis is still missing.

        Indexing happens on every pass (it is two cheap ZADDs that also refresh
        the TTL), while overlays are only fetched when absent — so a re-scan of
        an already-mirrored cycle costs no S3 GETs.
        """
        assert self._client is not None and self._redis_client is not None

        await self._redis_client.add_gfs_index(
            product.product_id,
            cycle,
            fxxx,
            cycle_score=_cycle_score(cycle),
            step_score=_step_score(fxxx),
            ttl=self._settings.gfs_geojson_ttl,
        )

        # Barbs are excluded on purpose: they live one object per tile and are
        # served straight from S3.
        layers = list(product.layers)
        await self._redis_client.add_gfs_layers(
            product.product_id, cycle, fxxx, layers, ttl=self._settings.gfs_geojson_ttl
        )

        copied = 0
        for layer in layers:
            if await self._redis_client.get_gfs_geojson(
                product.product_id, cycle, fxxx, layer
            ):
                continue
            key = S3Client.build_gfs_geojson_key(product.s3_segment, cycle, fxxx, layer)
            data = await self._client.download_tile(key)
            if not data:
                # A step whose COG exists but whose overlay has not been
                # uploaded yet: normal mid-run, picked up on a later pass.
                continue
            await self._redis_client.store_gfs_geojson(
                product.product_id,
                cycle,
                fxxx,
                layer,
                data,
                ttl=self._settings.gfs_geojson_ttl,
            )
            copied += 1
        return copied


def _cycle_score(cycle: str) -> float:
    """Sortable score for a cycle tag (`20260808T0600Z` → 202608080600)."""
    digits = "".join(ch for ch in cycle if ch.isdigit())
    return float(digits or 0)


def _step_score(fxxx: str) -> float:
    """Sortable score for a step tag (`f003` → 3)."""
    digits = fxxx[1:] if fxxx.startswith("f") else fxxx
    return float(digits) if digits.isdigit() else 0.0


# Singleton instance for use across the application
gfs_sync_service = GfsSyncService()
