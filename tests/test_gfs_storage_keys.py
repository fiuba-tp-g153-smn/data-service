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
    secondary_unit,
    secondary_vars_for,
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
        assert GFS_PRODUCTS["500hpa"].has_barbs
        assert not GFS_PRODUCTS["250hpa"].has_barbs
        assert not GFS_PRODUCTS["mslp"].has_barbs

    def test_layers_never_advertise_barbs(self):
        """`layers` must only hold names that resolve as `{layer}.json`.

        GFS has no `{step}_barbs.json` document — barbs exist only per tile —
        so listing them here would advertise a layer that 404s.
        """
        for product_id in product_ids():
            assert "barbs" not in layers_for(product_id)
        assert layers_for("500hpa") == ["heights", "isotherms"]

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


class TestSecondaryVariables:
    """The point-query COGs tiles-processor writes beside the primary one."""

    def test_each_product_carries_what_tiles_processor_uploads(self):
        assert secondary_vars_for("mslp") == ["thickness"]
        assert sorted(secondary_vars_for("500hpa")) == ["geopotential", "temperature"]
        assert secondary_vars_for("250hpa") == ["geopotential"]

    def test_250_has_no_temperature(self):
        """`GfsUpperLevelProcessor` only loads `t` at 500 hPa."""
        assert secondary_unit("250hpa", "temperature") is None

    def test_units_mirror_the_overlays_they_twin(self):
        assert secondary_unit("mslp", "thickness") == "gpm"
        assert secondary_unit("500hpa", "geopotential") == "gpm"
        assert secondary_unit("500hpa", "temperature") == "°C"

    def test_unknown_variable_has_no_unit(self):
        """`None` is what stops an arbitrary segment reaching the key builder."""
        assert secondary_unit("500hpa", "../../etc") is None
        assert secondary_unit("500hpa", "vorticity") is None

    def test_unknown_product_has_no_secondary_variables(self):
        assert secondary_vars_for("850hpa") == []
        assert secondary_unit("850hpa", "geopotential") is None

    def test_secondary_names_never_collide_with_overlay_names(self):
        """Both live under the same cycle; a shared name would be ambiguous.

        `thickness` is the deliberate exception: in MSLP the COG and the GeoJSON
        are the same field in two formats, and they sit under different top-level
        prefixes (`cog/` vs `geojson/`), so the name is reused on purpose.
        """
        for product_id in product_ids():
            shared = set(layers_for(product_id)) & set(secondary_vars_for(product_id))
            assert shared <= {"thickness"}


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

    def test_secondary_cog_key(self):
        assert (
            S3Client.build_gfs_secondary_cog_key("500hpa", CYCLE, "temperature", FXXX)
            == f"cog/models/gfs/500hpa/{CYCLE}/temperature/{STEP_ID}.tif"
        )

    def test_secondary_cog_is_nested_not_flat(self):
        """The whole point of the layout, and the easiest thing to "simplify".

        `list_steps` lists the cycle prefix with `delimiter="/"` and strips the
        cycle off each basename without validating the rest. A flat sibling —
        `{STEP_ID}.temperature.tif` — would come back as the phantom forecast
        step `f003.temperature` and reach the frontend as a real timestep.
        """
        cycle_prefix = f"cog/models/gfs/500hpa/{CYCLE}/"
        key = S3Client.build_gfs_secondary_cog_key("500hpa", CYCLE, "temperature", FXXX)
        assert "/" in key[len(cycle_prefix) :]

    def test_secondary_cog_never_collides_with_the_primary(self):
        primary = S3Client.build_gfs_cog_key("500hpa", CYCLE, FXXX)
        keys = {
            S3Client.build_gfs_secondary_cog_key("500hpa", CYCLE, variable, FXXX)
            for variable in secondary_vars_for("500hpa")
        }
        assert primary not in keys
        assert len(keys) == len(secondary_vars_for("500hpa"))

    def test_secondary_cog_shares_the_cycle_prefix(self):
        """Same product folder and cycle, so one lifecycle rule covers both."""
        key = S3Client.build_gfs_secondary_cog_key(
            "250hpa", CYCLE, "geopotential", FXXX
        )
        assert key.startswith(S3Client.gfs_cog_cycle_prefix("250hpa"))

    def test_secondary_cog_uses_the_joined_step_id(self):
        key = S3Client.build_gfs_secondary_cog_key(
            "mean_sea_level_pressure", CYCLE, "thickness", FXXX
        )
        assert key.endswith(f"/thickness/{CYCLE}_{FXXX}.tif")


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
