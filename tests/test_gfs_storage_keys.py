"""Unit tests for the GFS storage contracts: S3 keys, Redis keys, catalogue.

These pin the exact strings `tiles-processor` writes. A silent drift here is the
worst kind: the API keeps answering 200 with empty bodies instead of failing.
"""

from unittest.mock import AsyncMock

import pytest

from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.gfs_config import (
    GFS_BARB_ZOOM_LEVELS,
    GFS_PRODUCTS,
    GFS_ZOOM_MAX,
    GFS_ZOOM_MIN,
    get_product,
    layers_for,
    product_ids,
)

CYCLE = "20260808T0000Z"
FXXX = "f003"
STEP_ID = f"{CYCLE}_{FXXX}"


# ---------------------------------------------------------------------------
# Product catalogue
# ---------------------------------------------------------------------------


class TestCatalogue:
    def test_exposes_the_three_products(self):
        assert product_ids() == ["mslp", "500hpa", "250hpa"]

    def test_mslp_is_abbreviated_in_the_url_but_not_in_s3(self):
        """tiles-processor writes the long segment; the URL uses the short id."""
        product = get_product("mslp")
        assert product is not None
        assert product.s3_segment == "mean_sea_level_pressure"

    @pytest.mark.parametrize("product_id", ["500hpa", "250hpa"])
    def test_upper_level_ids_match_their_s3_segment(self, product_id):
        product = get_product(product_id)
        assert product is not None
        assert product.s3_segment == product_id

    def test_mslp_has_no_raster(self):
        """`slpb.gs` is a contour-only chart, so no tile pyramid is produced."""
        product = get_product("mslp")
        assert product is not None
        assert not product.has_tiles

    def test_only_500hpa_carries_barbs(self):
        assert layers_for("500hpa") == ["heights", "isotherms", "barbs"]
        assert "barbs" not in layers_for("250hpa")
        assert "barbs" not in layers_for("mslp")

    def test_units_match_what_the_cogs_hold(self):
        assert GFS_PRODUCTS["mslp"].unit == "hPa"
        assert GFS_PRODUCTS["500hpa"].unit == "kt"
        assert GFS_PRODUCTS["250hpa"].unit == "kt"

    def test_unknown_product_returns_none(self):
        assert get_product("850hpa") is None

    def test_zoom_range_matches_the_pyramid_tiles_processor_cuts(self):
        assert (GFS_ZOOM_MIN, GFS_ZOOM_MAX) == (3, 7)

    def test_barb_zooms_match_the_shared_strides(self):
        assert GFS_BARB_ZOOM_LEVELS == (2, 4, 6, 8)


# ---------------------------------------------------------------------------
# S3 keys
# ---------------------------------------------------------------------------


class TestS3Keys:
    """Every expected value is the literal key tiles-processor uploads."""

    def test_tile_key(self):
        assert (
            S3Client.build_gfs_tile_key("500hpa", CYCLE, FXXX, 5, 9, 17)
            == f"tiles/models/gfs/500hpa/{CYCLE}/{STEP_ID}/5/9/17.webp"
        )

    def test_cog_key(self):
        assert (
            S3Client.build_gfs_cog_key("250hpa", CYCLE, FXXX)
            == f"cog/models/gfs/250hpa/{CYCLE}/{STEP_ID}.tif"
        )

    def test_geojson_key(self):
        assert (
            S3Client.build_gfs_geojson_key("500hpa", CYCLE, FXXX, "isotherms")
            == f"geojson/models/gfs/500hpa/{CYCLE}/{STEP_ID}_isotherms.json"
        )

    def test_barb_tile_key(self):
        """Barbs are the one overlay stored per tile rather than per step."""
        assert (
            S3Client.build_gfs_barb_tile_key("500hpa", CYCLE, FXXX, 4, 5, 9)
            == f"geojson/models/gfs/500hpa/{CYCLE}/{STEP_ID}_barbs/4/5/9.json"
        )

    def test_mslp_keys_use_the_long_s3_segment(self):
        segment = GFS_PRODUCTS["mslp"].s3_segment
        key = S3Client.build_gfs_geojson_key(segment, CYCLE, FXXX, "isobars")
        assert key.startswith("geojson/models/gfs/mean_sea_level_pressure/")

    def test_step_id_joins_cycle_and_step(self):
        """The API splits them; tiles-processor names objects with them joined."""
        cog = S3Client.build_gfs_cog_key("500hpa", CYCLE, FXXX)
        assert cog.endswith(f"/{CYCLE}/{CYCLE}_{FXXX}.tif")

    def test_cog_cycle_prefix_is_listable(self):
        prefix = S3Client.gfs_cog_cycle_prefix("500hpa")
        assert prefix == "cog/models/gfs/500hpa/"
        assert S3Client.build_gfs_cog_key("500hpa", CYCLE, FXXX).startswith(prefix)

    def test_every_product_builds_a_distinct_cog_prefix(self):
        prefixes = {
            S3Client.gfs_cog_cycle_prefix(p.s3_segment) for p in GFS_PRODUCTS.values()
        }
        assert len(prefixes) == len(GFS_PRODUCTS)


# ---------------------------------------------------------------------------
# Redis keys
# ---------------------------------------------------------------------------


def _client() -> RedisClient:
    client = RedisClient("redis://localhost:6379/0")
    client._redis = AsyncMock()
    return client


class TestRedisGeoJson:
    @pytest.mark.asyncio
    async def test_store_with_ttl(self):
        client = _client()
        await client.store_gfs_geojson("mslp", CYCLE, FXXX, "isobars", b"x", ttl=64800)
        client._redis.set.assert_awaited_once_with(
            f"geojson:gfs:mslp/{CYCLE}/{FXXX}/isobars", b"x", ex=64800
        )

    @pytest.mark.asyncio
    async def test_store_without_ttl(self):
        client = _client()
        await client.store_gfs_geojson("mslp", CYCLE, FXXX, "isobars", b"x")
        client._redis.set.assert_awaited_once_with(
            f"geojson:gfs:mslp/{CYCLE}/{FXXX}/isobars", b"x"
        )

    @pytest.mark.asyncio
    async def test_get_uses_the_same_key_as_store(self):
        client = _client()
        client._redis.get = AsyncMock(return_value=b'{"type":"FeatureCollection"}')
        result = await client.get_gfs_geojson("500hpa", CYCLE, FXXX, "heights")
        client._redis.get.assert_awaited_once_with(
            f"geojson:gfs:500hpa/{CYCLE}/{FXXX}/heights"
        )
        assert result == b'{"type":"FeatureCollection"}'


class TestRedisTiles:
    @pytest.mark.asyncio
    async def test_store_and_get_share_the_key(self):
        client = _client()
        client._redis.get = AsyncMock(return_value=b"webp")
        await client.store_gfs_tile("500hpa", CYCLE, FXXX, 5, 9, 17, b"webp", ttl=10)
        await client.get_gfs_tile("500hpa", CYCLE, FXXX, 5, 9, 17)
        expected = f"tile:gfs:500hpa/{CYCLE}/{FXXX}/5/9/17"
        client._redis.set.assert_awaited_once_with(expected, b"webp", ex=10)
        client._redis.get.assert_awaited_once_with(expected)

    @pytest.mark.asyncio
    async def test_products_do_not_collide(self):
        """500 and 250 render the same z/x/y; their cache keys must differ."""
        client = _client()
        await client.store_gfs_tile("500hpa", CYCLE, FXXX, 5, 9, 17, b"a")
        await client.store_gfs_tile("250hpa", CYCLE, FXXX, 5, 9, 17, b"b")
        keys = [call.args[0] for call in client._redis.set.await_args_list]
        assert len(set(keys)) == 2


class TestRedisIndexes:
    @pytest.mark.asyncio
    async def test_add_index_writes_both_sorted_sets_with_ttl(self):
        client = _client()
        pipe = AsyncMock()
        pipe.zadd = lambda *a, **k: None
        pipe.expire = lambda *a, **k: None
        client._redis.pipeline = AsyncMock(return_value=pipe)

        await client.add_gfs_index("500hpa", CYCLE, FXXX, 1.0, 3.0, 64800)

        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cycles_come_back_newest_first(self):
        client = _client()
        client._redis.zrevrange = AsyncMock(return_value=[b"20260808T0600Z", b"x"])
        result = await client.get_gfs_cycles("500hpa")
        client._redis.zrevrange.assert_awaited_once_with("idx:gfs:500hpa:cycles", 0, -1)
        assert result == ["20260808T0600Z", "x"]

    @pytest.mark.asyncio
    async def test_steps_come_back_ascending(self):
        client = _client()
        client._redis.zrange = AsyncMock(return_value=[b"f000", b"f003"])
        result = await client.get_gfs_steps("500hpa", CYCLE)
        client._redis.zrange.assert_awaited_once_with(
            f"idx:gfs:500hpa:{CYCLE}:steps", 0, -1
        )
        assert result == ["f000", "f003"]

    @pytest.mark.asyncio
    async def test_layers_are_sorted(self):
        client = _client()
        client._redis.smembers = AsyncMock(return_value={b"isotherms", b"heights"})
        assert await client.get_gfs_layers("500hpa", CYCLE, FXXX) == [
            "heights",
            "isotherms",
        ]

    @pytest.mark.asyncio
    async def test_empty_layer_list_writes_nothing(self):
        client = _client()
        await client.add_gfs_layers("mslp", CYCLE, FXXX, [], 10)
        client._redis.pipeline.assert_not_awaited()
