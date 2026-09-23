"""Unit tests for the bundled product-availability snapshot.

Two properties matter, and they pull against each other:
  * the snapshot must agree with an individual probe in every state — cold
    index, half-synced, warm — because it is what greys rows out;
  * answering for ~125 products must cost a bounded number of lookups.

Reading through the same strategies the endpoints read through is what buys the
first; `_LOOKUP_CONCURRENCY` is what bounds the second.
"""

import asyncio
from typing import Dict, List

import pytest

from services.product_availability_service import (
    EcmwfTpAvailability,
    GfsAvailability,
    ProductAvailabilityService,
    RadarAvailability,
    SatelliteAvailability,
    WrfAvailability,
    _LOOKUP_CONCURRENCY,
    build_contributors,
)

CHANNEL_DIRS = ["goes19/abi/c13", "goes19/glm/fed"]
GFS_IDS = ["mean-sea-level-pressure", "geopotential-500hpa"]


class FakeRadarStrategy:
    """Radar strategy double: whatever it returns IS what the endpoint returns."""

    def __init__(self, fleet: Dict[str, Dict[str, Dict[str, List[str]]]]):
        self._fleet = fleet
        self.inflight = 0
        self.peak = 0

    async def _tracked(self, value):
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)
        try:
            await asyncio.sleep(0)
            return value
        finally:
            self.inflight -= 1

    async def list_radars(self) -> List[str]:
        return sorted(self._fleet)

    async def list_variables(self, radar_id: str) -> List[str]:
        return await self._tracked(sorted(self._fleet.get(radar_id, {})))

    async def list_elevations(self, radar_id: str, variable_id: str) -> List[str]:
        return await self._tracked(
            sorted(self._fleet.get(radar_id, {}).get(variable_id, {}))
        )

    async def list_tilesets(self, radar_id, variable_id, elevation_id) -> List[str]:
        return await self._tracked(
            list(
                self._fleet.get(radar_id, {}).get(variable_id, {}).get(elevation_id, [])
            )
        )


class FakeListStrategy:
    """Stands in for the satellite / ECMWF / WRF / GFS listing strategies."""

    def __init__(self, entries: Dict[str, List[str]]):
        self._entries = entries
        self.calls: List[str] = []

    async def get_tilesets(self, channel_dir: str) -> List[str]:
        self.calls.append(channel_dir)
        return list(self._entries.get(channel_dir, []))

    async def list_forecasts(self) -> List[str]:
        return list(self._entries.get("forecasts", []))

    async def list_products(self) -> List[str]:
        return sorted(self._entries)

    async def list_init_runs(self, product_id: str) -> List[str]:
        return list(self._entries.get(product_id, []))

    async def list_cycles(self, product_id: str) -> List[str]:
        return list(self._entries.get(product_id, []))


class TestRadarAvailability:
    @pytest.mark.asyncio
    async def test_reports_only_combinations_that_have_tilesets(self):
        strategy = FakeRadarStrategy(
            {
                "RMA1": {"dbzh": {"elev0": ["ts1"]}, "kdp": {"elev0": []}},
                "RMA2": {"dbzh": {"elev0": ["ts1", "ts2"]}},
            }
        )

        result = await RadarAvailability(strategy).available()

        assert result == [
            "radar-sinarame/RMA1/dbzh/elev0",
            "radar-sinarame/RMA2/dbzh/elev0",
        ]

    @pytest.mark.asyncio
    async def test_an_empty_fleet_reports_nothing(self):
        assert await RadarAvailability(FakeRadarStrategy({})).available() == []

    @pytest.mark.asyncio
    async def test_the_whole_fleet_stays_within_the_concurrency_bound(self):
        """18 radars x 6 variables x 3 elevations must not go in flight at once."""
        fleet = {
            f"RMA{n}": {
                v: {f"elev{e}": ["ts"] for e in range(3)}
                for v in ("dbzh", "dbzh-450km", "kdp", "vrad", "rhohv", "zdr")
            }
            for n in range(1, 19)
        }
        strategy = FakeRadarStrategy(fleet)

        result = await RadarAvailability(strategy).available()

        assert len(result) == 18 * 6 * 3
        assert strategy.peak <= _LOOKUP_CONCURRENCY


class TestOtherDomains:
    @pytest.mark.asyncio
    async def test_satellite_keys_are_the_product_paths(self):
        strategy = FakeListStrategy({"goes19/abi/c13": ["ts1"], "goes19/glm/fed": []})

        result = await SatelliteAvailability(strategy, CHANNEL_DIRS).available()

        assert result == ["goes19/abi/c13"]

    @pytest.mark.asyncio
    async def test_ecmwf_reports_its_single_product_when_it_has_forecasts(self):
        assert await EcmwfTpAvailability(
            FakeListStrategy({"forecasts": ["20260913T0000Z"]})
        ).available() == ["ecmwf-ifs/total-precipitation"]
        assert await EcmwfTpAvailability(FakeListStrategy({})).available() == []

    @pytest.mark.asyncio
    async def test_wrf_products_are_discovered_then_checked(self):
        strategy = FakeListStrategy({"granizo": ["20260913_060000"], "mucape": []})

        result = await WrfAvailability(strategy).available()

        assert result == ["wrf-arg4k/granizo"]

    @pytest.mark.asyncio
    async def test_gfs_reports_catalogued_products_with_cycles(self):
        strategy = FakeListStrategy({"mean-sea-level-pressure": ["c1"]})

        result = await GfsAvailability(strategy, GFS_IDS).available()

        assert result == ["gfs/mean-sea-level-pressure"]


class TestAgreesWithTheIndividualProbe:
    """The property the whole design exists to guarantee.

    Two earlier versions read the Redis indexes directly while the endpoints
    read Redis-then-S3. That second source of truth made the snapshot either a
    liar (greying out products whose data was in S3 awaiting a sync) or mute
    (confirming nothing, so the client probed all ~125 anyway). Reading through
    the same strategy is what makes these agree by construction.
    """

    @pytest.mark.asyncio
    async def test_a_cold_index_still_reports_what_s3_would_serve(self):
        """The strategies fall back to S3, so `available` is populated even
        before the sync loop has written a single index entry."""
        # This double answers as a cold-Redis strategy does: from S3.
        strategy = FakeRadarStrategy({"RMA1": {"dbzh": {"elev0": ["from-s3"]}}})

        result = await RadarAvailability(strategy).available()

        assert result == ["radar-sinarame/RMA1/dbzh/elev0"]

    @pytest.mark.asyncio
    async def test_absence_means_the_probe_would_also_find_nothing(self):
        """So the client can trust absence and skip the probe entirely."""
        fleet = {"RMA1": {"dbzh": {"elev0": []}, "kdp": {"elev0": []}}}
        strategy = FakeRadarStrategy(fleet)

        result = await RadarAvailability(strategy).available()

        assert result == []
        # Same strategy, same answer: an individual probe finds nothing either.
        assert await strategy.list_tilesets("RMA1", "dbzh", "elev0") == []


class _Contributor:
    """Contributor double that counts how often it is actually consulted."""

    def __init__(self, domain: str, result: list):
        self.domain = domain
        self._result = result
        self.calls = 0

    async def available(self) -> list:
        self.calls += 1
        return list(self._result)


class TestSnapshotAggregation:
    @pytest.mark.asyncio
    async def test_every_contributor_is_merged_and_its_domain_listed(self):
        service = ProductAvailabilityService(
            [
                _Contributor("radar-sinarame", ["radar-sinarame/RMA1/dbzh/elev0"]),
                _Contributor("gfs", ["gfs/geopotential-500hpa"]),
            ],
            ttl_seconds=10.0,
        )

        snapshot = await service.snapshot()

        assert snapshot.available == [
            "gfs/geopotential-500hpa",
            "radar-sinarame/RMA1/dbzh/elev0",
        ]
        assert snapshot.domains == ["gfs", "radar-sinarame"]

    @pytest.mark.asyncio
    async def test_concurrent_callers_share_one_sweep(self):
        """The memo is why this scales with clients, not clients x products."""
        contributor = _Contributor("gfs", ["gfs/x"])
        service = ProductAvailabilityService([contributor], ttl_seconds=60.0)

        await asyncio.gather(*(service.snapshot() for _ in range(25)))

        assert contributor.calls == 1

    @pytest.mark.asyncio
    async def test_the_memo_expires(self):
        contributor = _Contributor("gfs", ["gfs/x"])
        service = ProductAvailabilityService([contributor], ttl_seconds=0.0)

        await service.snapshot()
        await service.snapshot()

        assert contributor.calls == 2

    @pytest.mark.asyncio
    async def test_reconfiguring_drops_the_memo(self):
        stale = _Contributor("gfs", ["gfs/x"])
        service = ProductAvailabilityService([stale], ttl_seconds=60.0)
        await service.snapshot()

        fresh = _Contributor("wrf-arg4k", ["wrf-arg4k/granizo"])
        service.configure([fresh], ttl_seconds=60.0)

        assert (await service.snapshot()).available == ["wrf-arg4k/granizo"]


class TestContributorRegistry:
    def test_every_probeable_domain_is_registered(self):
        """The frontend greys rows off this list; a missing domain greys wrongly."""
        contributors = build_contributors(
            radar_strategy=FakeRadarStrategy({}),
            inta_radar_strategy=FakeRadarStrategy({}),
            satellite_strategy=FakeListStrategy({}),
            satellite_channel_dirs=CHANNEL_DIRS,
            ecmwf_tp_strategy=FakeListStrategy({}),
            wrf_strategy=FakeListStrategy({}),
            gfs_strategy=FakeListStrategy({}),
            gfs_product_ids=GFS_IDS,
        )

        assert sorted(c.domain for c in contributors) == [
            "ecmwf-ifs",
            "gfs",
            "goes19",
            # One contributor per radar fleet: the frontend probes the same
            # path it would have requested, and the two fleets are two paths.
            "radar-inta",
            "radar-sinarame",
            "wrf-arg4k",
        ]
