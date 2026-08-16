"""Unit tests for the GFS background sync loop.

Built via `__new__()` with mock S3/Redis clients so no env or real loop
machinery is involved — the generic loop behaviour is covered by
`test_base_sync_service.py`.
"""

from unittest.mock import AsyncMock

import pytest

from services.gfs_config import GFS_500HPA, GFS_MSLP
from services.gfs_sync_service import GfsSyncService
from settings import Settings

CYCLE_NEW = "20260808T0600Z"
CYCLE_OLD = "20260808T0000Z"
MSLP_PREFIX = "cog/models/gfs/mean_sea_level_pressure/"


def _make_settings(cycles_to_keep: int = 2) -> Settings:
    settings = Settings.__new__(Settings)
    settings.gfs_cycles_to_keep = cycles_to_keep
    settings.gfs_geojson_ttl = 64800
    settings.gfs_tile_ttl = 64800
    settings.s3_max_concurrent_downloads = 5
    return settings


def _make_service(mock_s3, mock_redis, settings=None) -> GfsSyncService:
    service = GfsSyncService.__new__(GfsSyncService)
    service._settings = settings or _make_settings()
    service._client = mock_s3
    service._redis_client = mock_redis
    return service


def _s3(cycles=None, basenames=None, payload=b'{"type":"FeatureCollection"}'):
    # `is None` rather than falsiness: an explicitly empty list is a meaningful
    # case here (it is what a transient S3 error looks like).
    resolved_cycles = [CYCLE_NEW] if cycles is None else cycles
    s3 = AsyncMock()
    s3.get_subdirectories = AsyncMock(
        return_value=[f"{MSLP_PREFIX}{c}/" for c in resolved_cycles]
    )
    s3.list_object_basenames = AsyncMock(
        return_value=basenames if basenames is not None else [f"{CYCLE_NEW}_f000"]
    )
    s3.download_tile = AsyncMock(return_value=payload)
    return s3


def _redis(existing_geojson=None):
    redis = AsyncMock()
    redis.get_gfs_geojson = AsyncMock(return_value=existing_geojson)
    return redis


class TestCycleDiscovery:
    @pytest.mark.asyncio
    async def test_cycles_come_from_the_cog_prefix(self):
        """`mslp` has no raster; listing tiles would make it look empty."""
        s3, redis = _s3(), _redis()
        await _make_service(s3, redis)._active_cycles(GFS_MSLP)
        s3.get_subdirectories.assert_awaited_once_with(MSLP_PREFIX)

    @pytest.mark.asyncio
    async def test_newest_cycles_first(self):
        s3, redis = _s3(cycles=[CYCLE_OLD, CYCLE_NEW]), _redis()
        cycles = await _make_service(s3, redis)._active_cycles(GFS_MSLP)
        assert cycles == [CYCLE_NEW, CYCLE_OLD]

    @pytest.mark.asyncio
    async def test_capped_at_cycles_to_keep(self):
        """Bounds the scan as runs pile up in S3."""
        s3, redis = _s3(cycles=[CYCLE_OLD, CYCLE_NEW, "20260807T1800Z"]), _redis()
        service = _make_service(s3, redis, _make_settings(cycles_to_keep=2))
        assert await service._active_cycles(GFS_MSLP) == [CYCLE_NEW, CYCLE_OLD]

    @pytest.mark.asyncio
    async def test_no_cycles_means_no_work(self):
        s3, redis = _s3(cycles=[]), _redis()
        assert await _make_service(s3, redis)._sync_product(GFS_MSLP) == 0
        redis.add_gfs_index.assert_not_awaited()


class TestStepDiscovery:
    @pytest.mark.asyncio
    async def test_steps_strip_the_cycle_prefix(self):
        s3 = _s3(basenames=[f"{CYCLE_NEW}_f003", f"{CYCLE_NEW}_f000"])
        steps = await _make_service(s3, _redis())._list_steps(GFS_MSLP, CYCLE_NEW)
        assert steps == ["f000", "f003"]

    @pytest.mark.asyncio
    async def test_foreign_basenames_are_ignored(self):
        s3 = _s3(basenames=[f"{CYCLE_NEW}_f003", "20260101T0000Z_f009", "junk"])
        steps = await _make_service(s3, _redis())._list_steps(GFS_MSLP, CYCLE_NEW)
        assert steps == ["f003"]


class TestStepSync:
    @pytest.mark.asyncio
    async def test_indexes_the_step(self):
        redis = _redis()
        await _make_service(_s3(), redis)._sync_step(GFS_MSLP, CYCLE_NEW, "f003")
        redis.add_gfs_index.assert_awaited_once()
        kwargs = redis.add_gfs_index.await_args.kwargs
        assert kwargs["cycle_score"] == 202608080600.0
        assert kwargs["step_score"] == 3.0

    @pytest.mark.asyncio
    async def test_registers_only_single_file_layers(self):
        """Barbs live one object per tile and are served straight from S3."""
        redis = _redis()
        await _make_service(_s3(), redis)._sync_step(GFS_500HPA, CYCLE_NEW, "f003")
        layers = redis.add_gfs_layers.await_args.args[3]
        assert layers == ["heights", "isotherms"]
        assert "barbs" not in layers

    @pytest.mark.asyncio
    async def test_mirrors_each_missing_overlay(self):
        s3, redis = _s3(), _redis(existing_geojson=None)
        copied = await _make_service(s3, redis)._sync_step(GFS_MSLP, CYCLE_NEW, "f003")
        assert copied == 2  # isobars + thickness
        assert redis.store_gfs_geojson.await_count == 2

    @pytest.mark.asyncio
    async def test_downloads_the_expected_key(self):
        s3, redis = _s3(), _redis()
        await _make_service(s3, redis)._sync_step(GFS_MSLP, CYCLE_NEW, "f003")
        keys = [c.args[0] for c in s3.download_tile.await_args_list]
        assert (
            f"geojson/models/gfs/mean_sea_level_pressure/{CYCLE_NEW}/"
            f"{CYCLE_NEW}_f003_isobars.json" in keys
        )

    @pytest.mark.asyncio
    async def test_already_mirrored_overlays_cost_no_s3_get(self):
        """A re-scan of a warm cycle must not re-download anything."""
        s3, redis = _s3(), _redis(existing_geojson=b"already-here")
        copied = await _make_service(s3, redis)._sync_step(GFS_MSLP, CYCLE_NEW, "f003")
        assert copied == 0
        s3.download_tile.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_index_is_refreshed_even_when_nothing_is_copied(self):
        """The ZADDs also refresh the TTL, so they run on every pass."""
        redis = _redis(existing_geojson=b"warm")
        await _make_service(_s3(), redis)._sync_step(GFS_MSLP, CYCLE_NEW, "f003")
        redis.add_gfs_index.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_overlay_not_yet_uploaded_is_skipped_quietly(self):
        """Normal mid-run: the COG exists before its GeoJSONs do."""
        s3, redis = _s3(payload=None), _redis()
        copied = await _make_service(s3, redis)._sync_step(GFS_MSLP, CYCLE_NEW, "f003")
        assert copied == 0
        redis.store_gfs_geojson.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unavailable_overlay_is_not_indexed(self):
        """The index must describe what is retrievable, not what the catalogue
        says: a step listing an overlay that is not there makes the frontend
        fetch a 404 for the whole life of the cycle."""
        s3, redis = _s3(payload=None), _redis()
        await _make_service(s3, redis)._sync_step(GFS_MSLP, CYCLE_NEW, "f003")
        # Nothing retrievable -> nothing registered at all.
        assert redis.add_gfs_layers.await_args.args[3] == []

    @pytest.mark.asyncio
    async def test_only_the_retrievable_overlays_are_indexed(self):
        """`isobars` is already mirrored, `thickness` is still missing."""
        s3, redis = _s3(payload=None), _redis(existing_geojson=None)

        async def _only_isobars(product_id, cycle, fxxx, layer):
            return b"warm" if layer == "isobars" else None

        redis.get_gfs_geojson = AsyncMock(side_effect=_only_isobars)
        await _make_service(s3, redis)._sync_step(GFS_MSLP, CYCLE_NEW, "f003")
        assert redis.add_gfs_layers.await_args.args[3] == ["isobars"]

    @pytest.mark.asyncio
    async def test_a_freshly_copied_overlay_is_indexed(self):
        s3, redis = _s3(), _redis(existing_geojson=None)
        await _make_service(s3, redis)._sync_step(GFS_MSLP, CYCLE_NEW, "f003")
        assert redis.add_gfs_layers.await_args.args[3] == ["isobars", "thickness"]


class TestPruning:
    @pytest.mark.asyncio
    async def test_prunes_to_the_active_cycles(self):
        s3, redis = _s3(cycles=[CYCLE_NEW, CYCLE_OLD]), _redis()
        await _make_service(s3, redis)._sync_product(GFS_MSLP)
        redis.prune_gfs_cycles.assert_awaited_once_with("mslp", [CYCLE_NEW, CYCLE_OLD])

    @pytest.mark.asyncio
    async def test_empty_listing_does_not_wipe_the_index(self):
        """A transient S3 error yields []; pruning on that would erase the index."""
        s3, redis = _s3(cycles=[]), _redis()
        await _make_service(s3, redis)._sync_product(GFS_MSLP)
        redis.prune_gfs_cycles.assert_not_awaited()


class TestFullPass:
    @pytest.mark.asyncio
    async def test_covers_every_product(self):
        s3, redis = _s3(), _redis()
        synced, errors = await _make_service(s3, redis)._sync_gfs()
        assert errors == 0
        products = {c.args[0] for c in redis.add_gfs_index.await_args_list}
        assert products == {"mslp", "500hpa", "250hpa"}
        assert synced > 0

    @pytest.mark.asyncio
    async def test_one_failing_product_does_not_stop_the_others(self):
        s3, redis = _s3(), _redis()
        original = s3.get_subdirectories

        async def fail_on_mslp(prefix):
            if "mean_sea_level_pressure" in prefix:
                raise RuntimeError("S3 down")
            return await original(prefix)

        s3.get_subdirectories = fail_on_mslp

        synced, errors = await _make_service(s3, redis)._sync_gfs()
        assert errors == 1
        products = {c.args[0] for c in redis.add_gfs_index.await_args_list}
        assert "mslp" not in products
        assert {"500hpa", "250hpa"} <= products
        assert synced > 0

    @pytest.mark.asyncio
    async def test_uninitialised_clients_raise(self):
        service = _make_service(None, None)
        with pytest.raises(RuntimeError, match="not initialized"):
            await service._sync_gfs()
