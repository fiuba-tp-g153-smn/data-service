"""Unit tests for the bundled product-availability snapshot.

The point of this service is that answering for the whole fleet costs a bounded
number of round trips rather than one per product, so the call counts are as
much of the contract as the values.
"""

import asyncio
from unittest.mock import AsyncMock

import pytest

from services.product_availability_service import (
    EcmwfTpAvailability,
    GfsAvailability,
    ProductAvailabilityService,
    RadarAvailability,
    SatelliteAvailability,
    WrfAvailability,
    build_contributors,
)

CHANNEL_DIRS = ["goes19/abi/c13", "goes19/glm/fed"]
GFS_IDS = ["mean-sea-level-pressure", "geopotential-500hpa"]


def _redis() -> AsyncMock:
    redis = AsyncMock()
    redis.get_radar_radars = AsyncMock(return_value=[])
    redis.get_radar_variables_bulk = AsyncMock(return_value={})
    redis.get_radar_elevations_bulk = AsyncMock(return_value={})
    redis.count_radar_tilesets_bulk = AsyncMock(return_value={})
    redis.count_satellite_tilesets_bulk = AsyncMock(return_value={})
    redis.get_ecmwf_tp_forecasts = AsyncMock(return_value=[])
    redis.get_wrf_products = AsyncMock(return_value=[])
    redis.count_wrf_init_runs_bulk = AsyncMock(return_value={})
    redis.count_gfs_cycles_bulk = AsyncMock(return_value={})
    return redis


class TestRadarAvailability:
    @pytest.mark.asyncio
    async def test_the_whole_fleet_costs_three_round_trips(self):
        """18 radars x 6 variables is ~108 products; it must not be ~108 calls."""
        redis = _redis()
        radars = [f"RMA{n}" for n in range(1, 19)]
        variables = ["dbzh", "dbzh-450km", "kdp", "vrad", "rhohv", "zdr"]
        redis.get_radar_radars = AsyncMock(return_value=radars)
        redis.get_radar_variables_bulk = AsyncMock(
            return_value={r: list(variables) for r in radars}
        )
        redis.get_radar_elevations_bulk = AsyncMock(
            side_effect=lambda pairs: {p: ["elev0"] for p in pairs}
        )
        redis.count_radar_tilesets_bulk = AsyncMock(
            side_effect=lambda combos: {c: 1 for c in combos}
        )

        result = await RadarAvailability(redis).availability()

        assert len(result) == 18 * 6
        assert result["radar-sinarame/RMA2/dbzh/elev0"] is True
        # variables, elevations, counts — one bulk call each, whatever the size.
        assert redis.get_radar_variables_bulk.await_count == 1
        assert redis.get_radar_elevations_bulk.await_count == 1
        assert redis.count_radar_tilesets_bulk.await_count == 1

    @pytest.mark.asyncio
    async def test_a_combination_with_no_tilesets_reports_false(self):
        redis = _redis()
        redis.get_radar_radars = AsyncMock(return_value=["RMA1"])
        redis.get_radar_variables_bulk = AsyncMock(return_value={"RMA1": ["dbzh"]})
        redis.get_radar_elevations_bulk = AsyncMock(
            return_value={("RMA1", "dbzh"): ["elev0"]}
        )
        redis.count_radar_tilesets_bulk = AsyncMock(
            return_value={("RMA1", "dbzh", "elev0"): 0}
        )

        result = await RadarAvailability(redis).availability()

        assert result == {"radar-sinarame/RMA1/dbzh/elev0": False}

    @pytest.mark.asyncio
    async def test_an_empty_index_asks_nothing_further(self):
        redis = _redis()

        assert await RadarAvailability(redis).availability() == {}
        redis.get_radar_variables_bulk.assert_not_awaited()
        redis.count_radar_tilesets_bulk.assert_not_awaited()


class TestStaticCatalogueDomains:
    @pytest.mark.asyncio
    async def test_satellite_keys_are_the_product_paths(self):
        redis = _redis()
        redis.count_satellite_tilesets_bulk = AsyncMock(
            return_value={"goes19/abi/c13": 4, "goes19/glm/fed": 0}
        )

        result = await SatelliteAvailability(redis, CHANNEL_DIRS).availability()

        assert result == {"goes19/abi/c13": True, "goes19/glm/fed": False}

    @pytest.mark.asyncio
    async def test_ecmwf_reports_its_single_product(self):
        redis = _redis()
        redis.get_ecmwf_tp_forecasts = AsyncMock(return_value=["20260913T0000Z"])

        result = await EcmwfTpAvailability(redis).availability()

        assert result == {"ecmwf-ifs/total-precipitation": True}

    @pytest.mark.asyncio
    async def test_gfs_reports_every_catalogued_product(self):
        redis = _redis()
        redis.count_gfs_cycles_bulk = AsyncMock(
            return_value={"mean-sea-level-pressure": 2, "geopotential-500hpa": 0}
        )

        result = await GfsAvailability(redis, GFS_IDS).availability()

        assert result == {
            "gfs/mean-sea-level-pressure": True,
            "gfs/geopotential-500hpa": False,
        }


class TestWrfAvailability:
    @pytest.mark.asyncio
    async def test_products_come_from_the_index(self):
        redis = _redis()
        redis.get_wrf_products = AsyncMock(return_value=["granizo", "mucape"])
        redis.count_wrf_init_runs_bulk = AsyncMock(
            return_value={"granizo": 3, "mucape": 0}
        )

        result = await WrfAvailability(redis).availability()

        assert result == {"wrf-arg4k/granizo": True, "wrf-arg4k/mucape": False}

    @pytest.mark.asyncio
    async def test_an_unwritten_index_reports_nothing_rather_than_all_empty(self):
        """Before the first sync, "no answer" beats "every product is empty".

        An empty map leaves the domain out of `domains`, which the client reads
        as unknown — greying every WRF row because the service just started
        would be a worse answer than saying nothing.
        """
        redis = _redis()

        assert await WrfAvailability(redis).availability() == {}
        redis.count_wrf_init_runs_bulk.assert_not_awaited()


class _Contributor:
    """Contributor double that counts how often it is actually consulted."""

    def __init__(self, domain: str, result: dict):
        self.domain = domain
        self._result = result
        self.calls = 0

    async def availability(self) -> dict:
        self.calls += 1
        return dict(self._result)


class TestSnapshotAggregation:
    @pytest.mark.asyncio
    async def test_every_contributor_is_merged_and_its_domain_listed(self):
        service = ProductAvailabilityService(
            [
                _Contributor(
                    "radar-sinarame", {"radar-sinarame/RMA1/dbzh/elev0": True}
                ),
                _Contributor("gfs", {"gfs/geopotential-500hpa": False}),
            ],
            ttl_seconds=10.0,
        )

        snapshot = await service.snapshot()

        assert snapshot.products == {
            "radar-sinarame/RMA1/dbzh/elev0": True,
            "gfs/geopotential-500hpa": False,
        }
        assert snapshot.domains == ["gfs", "radar-sinarame"]

    @pytest.mark.asyncio
    async def test_a_silent_domain_is_left_out_of_domains(self):
        """Absent-from-`domains` is how the client tells unknown from empty."""
        service = ProductAvailabilityService(
            [
                _Contributor("wrf-arg4k", {}),
                _Contributor("gfs", {"gfs/mean-sea-level-pressure": True}),
            ],
            ttl_seconds=10.0,
        )

        snapshot = await service.snapshot()

        assert snapshot.domains == ["gfs"]
        assert not [k for k in snapshot.products if k.startswith("wrf-arg4k/")]

    @pytest.mark.asyncio
    async def test_concurrent_callers_share_one_walk(self):
        """The memo is the reason this scales with clients, not clients x products."""
        contributor = _Contributor("gfs", {"gfs/x": True})
        service = ProductAvailabilityService([contributor], ttl_seconds=60.0)

        await asyncio.gather(*(service.snapshot() for _ in range(25)))

        assert contributor.calls == 1

    @pytest.mark.asyncio
    async def test_the_memo_expires(self):
        contributor = _Contributor("gfs", {"gfs/x": True})
        service = ProductAvailabilityService([contributor], ttl_seconds=0.0)

        await service.snapshot()
        await service.snapshot()

        assert contributor.calls == 2

    @pytest.mark.asyncio
    async def test_reconfiguring_drops_the_memo(self):
        stale = _Contributor("gfs", {"gfs/x": True})
        service = ProductAvailabilityService([stale], ttl_seconds=60.0)
        await service.snapshot()

        fresh = _Contributor("wrf-arg4k", {"wrf-arg4k/granizo": True})
        service.configure([fresh], ttl_seconds=60.0)

        assert (await service.snapshot()).products == {"wrf-arg4k/granizo": True}


class TestContributorRegistry:
    def test_every_probeable_domain_is_registered(self):
        """The frontend greys rows off this list; a missing domain greys wrongly."""
        contributors = build_contributors(_redis(), CHANNEL_DIRS, GFS_IDS)

        assert sorted(c.domain for c in contributors) == [
            "ecmwf-ifs",
            "gfs",
            "goes19",
            "radar-sinarame",
            "wrf-arg4k",
        ]
