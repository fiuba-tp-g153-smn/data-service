"""Unit tests for GfsService: listings, valid timestamps and product gating."""

import pytest

from services.gfs_service import GfsService, valid_timestamp

CYCLE = "20260808T0000Z"


class FakeStrategy:
    """Records calls and replays canned listings."""

    def __init__(self, cycles=None, steps=None, payload=b"data"):
        self._cycles = cycles if cycles is not None else [CYCLE]
        self._steps = steps if steps is not None else ["f000", "f003"]
        self._payload = payload
        self.calls: list[tuple] = []

    async def list_cycles(self, product_id):
        self.calls.append(("list_cycles", product_id))
        return list(self._cycles)

    async def list_steps(self, product_id, cycle):
        self.calls.append(("list_steps", product_id, cycle))
        return list(self._steps)

    async def list_layers(self, product_id, cycle, fxxx):
        return []

    async def get_tile(self, product_id, cycle, fxxx, z, x, y):
        self.calls.append(("get_tile", product_id))
        return self._payload

    async def get_geojson(self, product_id, cycle, fxxx, layer):
        self.calls.append(("get_geojson", product_id, layer))
        return self._payload

    async def get_barb_tile(self, product_id, cycle, fxxx, z, x, y):
        self.calls.append(("get_barb_tile", product_id))
        return self._payload


def _service(strategy=None) -> GfsService:
    service = GfsService()
    service.set_strategy(strategy or FakeStrategy())
    return service


# ---------------------------------------------------------------------------
# valid_timestamp
# ---------------------------------------------------------------------------


class TestValidTimestamp:
    """The frontend animates in valid time, so this must be exact."""

    def test_analysis_step_equals_the_cycle(self):
        assert valid_timestamp(CYCLE, "f000") == CYCLE

    def test_adds_the_forecast_offset(self):
        assert valid_timestamp(CYCLE, "f003") == "20260808T0300Z"

    def test_rolls_over_the_day(self):
        assert valid_timestamp("20260808T1800Z", "f012") == "20260809T0600Z"

    def test_handles_the_longest_range(self):
        assert valid_timestamp(CYCLE, "f144") == "20260814T0000Z"

    @pytest.mark.parametrize("bad", ["", "f3", "003", "fxxx", "f0003"])
    def test_malformed_step_returns_none(self, bad):
        assert valid_timestamp(CYCLE, bad) is None

    def test_malformed_cycle_returns_none(self):
        """A stray S3 key must not blow up the listing endpoint."""
        assert valid_timestamp("not-a-cycle", "f003") is None


# ---------------------------------------------------------------------------
# Listings
# ---------------------------------------------------------------------------


class TestListCycles:
    @pytest.mark.asyncio
    async def test_unknown_product_returns_none(self):
        assert await _service().list_cycles("850hpa") is None

    @pytest.mark.asyncio
    async def test_reports_step_count_per_cycle(self):
        strategy = FakeStrategy(cycles=[CYCLE], steps=["f000", "f003", "f006"])
        data = await _service(strategy).list_cycles("500hpa")
        assert data is not None
        assert data.cycles[0].cycle == CYCLE
        assert data.cycles[0].step_count == 3

    @pytest.mark.asyncio
    async def test_advertises_the_products_layers(self):
        data = await _service().list_cycles("500hpa")
        assert data is not None
        assert data.layers == ["heights", "isotherms", "barbs"]

    @pytest.mark.asyncio
    async def test_mslp_advertises_no_tile_pattern(self):
        """Contour-only product: the frontend must not build tile URLs for it."""
        data = await _service().list_cycles("mslp")
        assert data is not None
        assert data.tile_url_pattern is None

    @pytest.mark.asyncio
    async def test_raster_products_advertise_a_tile_pattern(self):
        data = await _service().list_cycles("250hpa")
        assert data is not None
        assert data.tile_url_pattern is not None
        assert "{z}/{x}/{y}.webp" in data.tile_url_pattern

    @pytest.mark.asyncio
    async def test_without_a_strategy_returns_an_empty_but_valid_payload(self):
        data = await GfsService().list_cycles("500hpa")
        assert data is not None
        assert data.cycles == []


class TestListSteps:
    @pytest.mark.asyncio
    async def test_unknown_product_returns_none(self):
        assert await _service().list_steps("850hpa", CYCLE) is None

    @pytest.mark.asyncio
    async def test_unknown_cycle_returns_none(self):
        assert (
            await _service(FakeStrategy(steps=[])).list_steps("500hpa", CYCLE) is None
        )

    @pytest.mark.asyncio
    async def test_each_step_carries_its_valid_timestamp(self):
        strategy = FakeStrategy(steps=["f000", "f003"])
        data = await _service(strategy).list_steps("500hpa", CYCLE)
        assert data is not None
        assert [s.valid_ts for s in data.steps] == [CYCLE, "20260808T0300Z"]

    @pytest.mark.asyncio
    async def test_steps_carry_the_products_layers(self):
        data = await _service().list_steps("250hpa", CYCLE)
        assert data is not None
        assert data.steps[0].layers == ["heights"]


# ---------------------------------------------------------------------------
# Product gating
# ---------------------------------------------------------------------------


class TestProductGating:
    """The service refuses combinations that cannot exist, without hitting S3."""

    @pytest.mark.asyncio
    async def test_mslp_has_no_tiles(self):
        strategy = FakeStrategy()
        result = await _service(strategy).get_tile_data("mslp", CYCLE, "f003", 5, 9, 17)
        assert result is None
        assert not any(c[0] == "get_tile" for c in strategy.calls)

    @pytest.mark.asyncio
    async def test_250hpa_has_no_barbs(self):
        strategy = FakeStrategy()
        result = await _service(strategy).get_barb_tile(
            "250hpa", CYCLE, "f003", 4, 5, 9
        )
        assert result is None
        assert not any(c[0] == "get_barb_tile" for c in strategy.calls)

    @pytest.mark.asyncio
    async def test_500hpa_serves_barbs(self):
        strategy = FakeStrategy()
        result = await _service(strategy).get_barb_tile(
            "500hpa", CYCLE, "f003", 4, 5, 9
        )
        assert result == b"data"

    @pytest.mark.asyncio
    async def test_layer_not_belonging_to_the_product_is_refused(self):
        """`isotherms` exists at 500 hPa but not at 250 hPa."""
        strategy = FakeStrategy()
        result = await _service(strategy).get_geojson(
            "250hpa", CYCLE, "f003", "isotherms"
        )
        assert result is None
        assert not any(c[0] == "get_geojson" for c in strategy.calls)

    @pytest.mark.asyncio
    async def test_barbs_are_not_reachable_as_a_single_file_overlay(self):
        """Barbs are per-tile; asking for `barbs.json` must not hit S3."""
        strategy = FakeStrategy()
        result = await _service(strategy).get_geojson("500hpa", CYCLE, "f003", "barbs")
        assert result is None

    @pytest.mark.asyncio
    async def test_valid_overlay_is_served(self):
        result = await _service().get_geojson("mslp", CYCLE, "f003", "thickness")
        assert result == b"data"

    @pytest.mark.asyncio
    async def test_unknown_product_never_reaches_the_strategy(self):
        strategy = FakeStrategy()
        service = _service(strategy)
        assert await service.get_tile_data("850hpa", CYCLE, "f003", 5, 9, 17) is None
        assert await service.get_geojson("850hpa", CYCLE, "f003", "heights") is None
        assert strategy.calls == []
