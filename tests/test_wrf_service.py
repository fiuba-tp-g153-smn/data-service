"""Unit tests for WrfService listings."""

import pytest

from services.wrf_service import WrfService

PRODUCT = "agua-precipitable"
INIT_TAG = "20260911_060000"


class FakeStrategy:
    """Records calls and replays canned listings."""

    def __init__(self, init_runs=None, steps=None):
        self._init_runs = init_runs if init_runs is not None else [INIT_TAG]
        self._steps = steps if steps is not None else ["F001", "F002"]
        self.calls: list[tuple] = []
        self.layers_by_step: dict[str, list[str]] = {}

    async def list_init_runs(self, product_id):
        self.calls.append(("list_init_runs", product_id))
        return list(self._init_runs)

    async def list_steps(self, product_id, init_tag):
        self.calls.append(("list_steps", product_id, init_tag))
        return list(self._steps)

    async def list_layers(self, product_id, init_tag, fxxx):
        self.calls.append(("list_layers", product_id, init_tag, fxxx))
        return list(self.layers_by_step.get(fxxx, []))

    async def list_layers_bulk(self, product_id, init_tag, steps):
        self.calls.append(("list_layers_bulk", product_id, init_tag, tuple(steps)))
        return {fxxx: list(self.layers_by_step.get(fxxx, [])) for fxxx in steps}


def _service(strategy=None) -> WrfService:
    service = WrfService()
    service.set_strategy(strategy or FakeStrategy())
    return service


class TestListSteps:
    @pytest.mark.asyncio
    async def test_unknown_init_run_is_not_found(self):
        service = _service(FakeStrategy(init_runs=[INIT_TAG]))

        assert await service.list_steps(PRODUCT, "20260101_000000") is None

    @pytest.mark.asyncio
    async def test_steps_carry_the_layers_the_index_reports(self):
        strategy = FakeStrategy(steps=["F001", "F002"])
        strategy.layers_by_step = {"F001": ["barbs", "contours"]}
        service = _service(strategy)

        data = await service.list_steps(PRODUCT, INIT_TAG)

        assert [(s.fxxx, list(s.layers)) for s in data.steps] == [
            ("F001", ["barbs", "contours"]),
            ("F002", []),  # not indexed yet — advertised as having none
        ]

    @pytest.mark.asyncio
    async def test_listing_does_not_fan_out_one_call_per_step(self):
        """The regression: 73 per-step reads exhausted the shared Redis pool.

        Guarding at the service level because this is where the fan-out lived,
        and where a well-meaning refactor would most plausibly reintroduce it.
        """
        steps = [f"F{h:03d}" for h in range(1, 74)]
        strategy = FakeStrategy(steps=steps)
        service = _service(strategy)

        data = await service.list_steps(PRODUCT, INIT_TAG)

        assert len(data.steps) == 73
        assert not [c for c in strategy.calls if c[0] == "list_layers"]
        bulk = [c for c in strategy.calls if c[0] == "list_layers_bulk"]
        assert bulk == [("list_layers_bulk", PRODUCT, INIT_TAG, tuple(steps))]

    @pytest.mark.asyncio
    async def test_no_strategy_returns_none(self):
        assert await WrfService().list_steps(PRODUCT, INIT_TAG) is None
