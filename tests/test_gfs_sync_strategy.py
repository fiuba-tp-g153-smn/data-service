"""Unit tests for the GFS read strategies (Redis/S3 fallback + lazy caching)."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from services.gfs_sync_strategy import GfsFullSyncStrategy, GfsOnDemandStrategy

CYCLE = "20260808T0000Z"
FXXX = "f003"


def _redis() -> AsyncMock:
    """Redis double that misses on every read by default."""
    redis = AsyncMock()
    redis.get_gfs_tile = AsyncMock(return_value=None)
    redis.get_gfs_geojson = AsyncMock(return_value=None)
    redis.get_gfs_cycles = AsyncMock(return_value=[])
    redis.get_gfs_steps = AsyncMock(return_value=[])
    redis.get_gfs_layers = AsyncMock(return_value=[])
    redis.get_cached_listing = AsyncMock(return_value=None)
    return redis


def _redis_with_listing_cache() -> AsyncMock:
    """Redis double whose listing cache actually round-trips.

    Needed wherever the code under test relies on a second reader observing the
    first writer — a plain AsyncMock would keep missing and hide the dedup.
    """
    redis = _redis()
    store: dict[str, bytes] = {}

    async def _get(cache_key):
        return store.get(cache_key)

    async def _set(cache_key, data, _ttl):
        store[cache_key] = data

    redis.get_cached_listing = AsyncMock(side_effect=_get)
    redis.cache_listing = AsyncMock(side_effect=_set)
    return redis


def _s3() -> AsyncMock:
    s3 = AsyncMock()
    s3.download_tile = AsyncMock(return_value=b"payload")
    s3.get_subdirectories = AsyncMock(return_value=[])
    s3.list_object_basenames = AsyncMock(return_value=[])
    return s3


async def _settle() -> None:
    """Let the fire-and-forget cache-write tasks run."""
    await asyncio.sleep(0)


class TestOnDemandTiles:
    @pytest.mark.asyncio
    async def test_redis_hit_skips_s3(self):
        redis, s3 = _redis(), _s3()
        redis.get_gfs_tile = AsyncMock(return_value=b"cached")
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.get_tile("500hpa", CYCLE, FXXX, 5, 9, 17) == b"cached"
        s3.download_tile.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_miss_falls_back_to_s3_with_the_right_key(self):
        redis, s3 = _redis(), _s3()
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.get_tile("500hpa", CYCLE, FXXX, 5, 9, 17) == b"payload"
        s3.download_tile.assert_awaited_once_with(
            f"tiles/models/gfs/500hpa/{CYCLE}/{CYCLE}_{FXXX}/5/9/17.webp"
        )

    @pytest.mark.asyncio
    async def test_s3_hit_is_cached_lazily(self):
        redis, s3 = _redis(), _s3()
        strategy = GfsOnDemandStrategy(redis, s3, 42, 10, 10)

        await strategy.get_tile("500hpa", CYCLE, FXXX, 5, 9, 17)
        await _settle()

        redis.store_gfs_tile.assert_awaited_once()
        assert redis.store_gfs_tile.await_args.kwargs["ttl"] == 42

    @pytest.mark.asyncio
    async def test_absent_tile_is_not_cached(self):
        redis, s3 = _redis(), _s3()
        s3.download_tile = AsyncMock(return_value=None)
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.get_tile("500hpa", CYCLE, FXXX, 5, 9, 17) is None
        await _settle()
        redis.store_gfs_tile.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_product_never_touches_s3(self):
        redis, s3 = _redis(), _s3()
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.get_tile("850hpa", CYCLE, FXXX, 5, 9, 17) is None
        s3.download_tile.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_without_s3_returns_none(self):
        strategy = GfsOnDemandStrategy(_redis(), None, 10, 10, 10)
        assert await strategy.get_tile("500hpa", CYCLE, FXXX, 5, 9, 17) is None


class TestOnDemandOverlays:
    @pytest.mark.asyncio
    async def test_geojson_uses_the_single_file_key(self):
        redis, s3 = _redis(), _s3()
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        await strategy.get_geojson("mslp", CYCLE, FXXX, "isobars")
        s3.download_tile.assert_awaited_once_with(
            f"geojson/models/gfs/mean_sea_level_pressure/{CYCLE}/"
            f"{CYCLE}_{FXXX}_isobars.json"
        )

    @pytest.mark.asyncio
    async def test_geojson_is_cached_with_its_own_ttl(self):
        redis, s3 = _redis(), _s3()
        strategy = GfsOnDemandStrategy(redis, s3, 10, 99, 10)

        await strategy.get_geojson("mslp", CYCLE, FXXX, "isobars")
        await _settle()
        assert redis.store_gfs_geojson.await_args.kwargs["ttl"] == 99

    @pytest.mark.asyncio
    async def test_barb_tile_uses_the_per_tile_key(self):
        redis, s3 = _redis(), _s3()
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        await strategy.get_barb_tile("500hpa", CYCLE, FXXX, 4, 5, 9)
        s3.download_tile.assert_awaited_once_with(
            f"geojson/models/gfs/500hpa/{CYCLE}/{CYCLE}_{FXXX}_barbs/4/5/9.json"
        )

    @pytest.mark.asyncio
    async def test_barb_tiles_are_not_cached(self):
        """One key per tile would fill Redis with single-use entries."""
        redis, s3 = _redis(), _s3()
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        await strategy.get_barb_tile("500hpa", CYCLE, FXXX, 4, 5, 9)
        await _settle()
        redis.store_gfs_geojson.assert_not_awaited()


class TestDeferredCacheWrites:
    """Tiles are cached off the request path, so a failure there must be loud
    but harmless: the reader still gets its bytes, and the loss is logged."""

    @pytest.mark.asyncio
    async def test_a_failing_write_does_not_break_the_read(self, caplog):
        redis, s3 = _redis(), _s3()
        redis.store_gfs_tile = AsyncMock(side_effect=OSError("redis down"))
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.get_tile("500hpa", CYCLE, FXXX, 5, 9, 17) == b"payload"
        await _settle()
        await _settle()
        assert "cache write failed" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_an_unexpected_write_failure_is_logged_not_swallowed(self, caplog):
        redis, s3 = _redis(), _s3()
        redis.store_gfs_tile = AsyncMock(side_effect=ValueError("bug"))
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        await strategy.get_tile("500hpa", CYCLE, FXXX, 5, 9, 17)
        await _settle()
        await _settle()
        assert "unexpected gfs cache-write failure" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_the_write_task_is_tracked_until_it_finishes(self):
        """An untracked task can be collected before it runs, silently dropping
        the write that is this domain's entire tile cache."""
        redis, s3 = _redis(), _s3()
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        await strategy.get_tile("500hpa", CYCLE, FXXX, 5, 9, 17)
        assert strategy._cache_tasks  # pylint: disable=protected-access
        await _settle()
        await _settle()
        assert not strategy._cache_tasks  # pylint: disable=protected-access
        redis.store_gfs_tile.assert_awaited_once()


class TestOnDemandListings:
    @pytest.mark.asyncio
    async def test_cycles_are_discovered_from_the_cog_prefix(self):
        """Not the tiles prefix: mslp has no raster and would look empty."""
        redis, s3 = _redis(), _s3()
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        await strategy.list_cycles("mslp")
        s3.get_subdirectories.assert_awaited_once_with(
            "cog/models/gfs/mean_sea_level_pressure/"
        )

    @pytest.mark.asyncio
    async def test_cycles_come_back_newest_first(self):
        redis, s3 = _redis(), _s3()
        s3.get_subdirectories = AsyncMock(
            return_value=[
                "cog/models/gfs/500hpa/20260808T0000Z/",
                "cog/models/gfs/500hpa/20260808T0600Z/",
            ]
        )
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.list_cycles("500hpa") == [
            "20260808T0600Z",
            "20260808T0000Z",
        ]

    @pytest.mark.asyncio
    async def test_steps_strip_the_cycle_prefix_off_the_basename(self):
        redis, s3 = _redis(), _s3()
        s3.list_object_basenames = AsyncMock(
            return_value=[f"{CYCLE}_f003", f"{CYCLE}_f000"]
        )
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.list_steps("500hpa", CYCLE) == ["f000", "f003"]

    @pytest.mark.asyncio
    async def test_foreign_basenames_are_ignored(self):
        """An object from another cycle under this prefix must not leak in."""
        redis, s3 = _redis(), _s3()
        s3.list_object_basenames = AsyncMock(
            return_value=[f"{CYCLE}_f003", "20260101T0000Z_f009", "garbage"]
        )
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.list_steps("500hpa", CYCLE) == ["f003"]

    @pytest.mark.asyncio
    async def test_listings_are_cached(self):
        redis, s3 = _redis(), _s3()
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 77)

        await strategy.list_cycles("500hpa")
        redis.cache_listing.assert_awaited_once()
        assert redis.cache_listing.await_args.args[2] == 77

    @pytest.mark.asyncio
    async def test_cached_listing_short_circuits_s3(self):
        redis, s3 = _redis(), _s3()
        redis.get_cached_listing = AsyncMock(return_value=json.dumps(["a"]).encode())
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.list_cycles("500hpa") == ["a"]
        s3.get_subdirectories.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_layers_are_discovered_from_s3_not_the_catalogue(self):
        """A cycle fills in gradually, so only what exists may be advertised."""
        redis, s3 = _redis(), _s3()
        s3.list_object_basenames = AsyncMock(
            return_value=[f"{CYCLE}_{FXXX}_heights", f"{CYCLE}_{FXXX}_isotherms"]
        )
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.list_layers("500hpa", CYCLE, FXXX) == [
            "heights",
            "isotherms",
        ]
        prefix = s3.list_object_basenames.await_args.args[0]
        assert prefix == f"geojson/models/gfs/500hpa/{CYCLE}/"
        # Delimited: the `{step}_barbs/z/x/y` subtrees hold thousands of keys.
        assert s3.list_object_basenames.await_args.kwargs["delimiter"] == "/"

    @pytest.mark.asyncio
    async def test_a_step_without_overlays_advertises_none(self):
        redis, s3 = _redis(), _s3()
        s3.list_object_basenames = AsyncMock(return_value=[f"{CYCLE}_f000_heights"])
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.list_layers("500hpa", CYCLE, "f003") == []

    @pytest.mark.asyncio
    async def test_barbs_are_never_reported_as_a_layer(self):
        """`{step}_barbs/` is a subtree, so a delimited LIST cannot surface it."""
        redis, s3 = _redis(), _s3()
        s3.list_object_basenames = AsyncMock(return_value=[f"{CYCLE}_{FXXX}_heights"])
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        assert "barbs" not in await strategy.list_layers("500hpa", CYCLE, FXXX)

    @pytest.mark.asyncio
    async def test_the_whole_cycle_costs_one_list(self):
        """`list_steps` fans out one call per step; they must share the LIST."""
        redis, s3 = _redis_with_listing_cache(), _s3()
        s3.list_object_basenames = AsyncMock(
            return_value=[f"{CYCLE}_f000_heights", f"{CYCLE}_f003_heights"]
        )
        strategy = GfsOnDemandStrategy(redis, s3, 10, 10, 10)

        results = await asyncio.gather(
            strategy.list_layers("500hpa", CYCLE, "f000"),
            strategy.list_layers("500hpa", CYCLE, "f003"),
        )
        assert results == [["heights"], ["heights"]]
        assert s3.list_object_basenames.await_count == 1


class TestFullSyncStrategy:
    @pytest.mark.asyncio
    async def test_listings_prefer_redis(self):
        redis, s3 = _redis(), _s3()
        redis.get_gfs_cycles = AsyncMock(return_value=[CYCLE])
        strategy = GfsFullSyncStrategy(redis, s3, 10, 10, 10)

        assert await strategy.list_cycles("500hpa") == [CYCLE]
        s3.get_subdirectories.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cold_index_falls_back_to_s3(self):
        redis, s3 = _redis(), _s3()
        s3.get_subdirectories = AsyncMock(
            return_value=[f"cog/models/gfs/500hpa/{CYCLE}/"]
        )
        strategy = GfsFullSyncStrategy(redis, s3, 10, 10, 10)

        assert await strategy.list_cycles("500hpa") == [CYCLE]

    @pytest.mark.asyncio
    async def test_geojson_prefers_redis(self):
        redis, s3 = _redis(), _s3()
        redis.get_gfs_geojson = AsyncMock(return_value=b"warm")
        strategy = GfsFullSyncStrategy(redis, s3, 10, 10, 10)

        assert await strategy.get_geojson("mslp", CYCLE, FXXX, "isobars") == b"warm"
        s3.download_tile.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_tiles_always_go_through_the_on_demand_path(self):
        """Tiles are never pre-synced, so there is no Redis index to consult."""
        redis, s3 = _redis(), _s3()
        strategy = GfsFullSyncStrategy(redis, s3, 10, 10, 10)

        assert await strategy.get_tile("500hpa", CYCLE, FXXX, 5, 9, 17) == b"payload"
        s3.download_tile.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_barbs_always_go_through_the_on_demand_path(self):
        redis, s3 = _redis(), _s3()
        strategy = GfsFullSyncStrategy(redis, s3, 10, 10, 10)

        assert (
            await strategy.get_barb_tile("500hpa", CYCLE, FXXX, 4, 5, 9) == b"payload"
        )
        s3.download_tile.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_without_s3_reads_degrade_to_none(self):
        strategy = GfsFullSyncStrategy(_redis(), None, 10, 10, 10)
        assert await strategy.get_tile("500hpa", CYCLE, FXXX, 5, 9, 17) is None
        assert await strategy.list_cycles("500hpa") == []
