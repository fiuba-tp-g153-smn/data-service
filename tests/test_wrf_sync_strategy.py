"""Unit tests for the WRF read strategies' layer listing.

An init run is hourly out to F073. `list_steps` used to hydrate overlays with
one `list_layers` per step, so a single listing took ~73 concurrent pooled
Redis connections against a cap of 100 — two concurrent requests exhausted a
pool shared by every domain and 500ed the whole service, not just WRF. These
pin the listing cost to something that does not scale with the forecast length.
"""

import asyncio
from unittest.mock import AsyncMock

import pytest

from services.wrf_sync_strategy import (
    _LAYER_DISCOVERY_CONCURRENCY,
    WrfFullSyncStrategy,
    WrfOnDemandStrategy,
)

PRODUCT = "agua-precipitable"
INIT_TAG = "20260911_060000"
STEPS = [f"F{h:03d}" for h in range(1, 74)]


def _redis() -> AsyncMock:
    """Redis double that misses on every read by default."""
    redis = AsyncMock()
    redis.get_wrf_layers = AsyncMock(return_value=[])
    redis.get_wrf_layers_bulk = AsyncMock(return_value={})
    redis.get_cached_listing = AsyncMock(return_value=None)
    redis.cache_listing = AsyncMock()
    return redis


def _s3(layers=("barbs",)) -> AsyncMock:
    s3 = AsyncMock()
    s3.list_wrf_layers = AsyncMock(return_value=list(layers))
    return s3


class TestFullSyncBulkListing:
    @pytest.mark.asyncio
    async def test_warm_index_is_one_bulk_read_for_the_whole_run(self):
        redis, s3 = _redis(), _s3()
        redis.get_wrf_layers_bulk = AsyncMock(
            return_value={fxxx: ["barbs"] for fxxx in STEPS}
        )
        strategy = WrfFullSyncStrategy(redis, s3, 10, 10, 10)

        result = await strategy.list_layers_bulk(PRODUCT, INIT_TAG, STEPS)

        assert result == {fxxx: ["barbs"] for fxxx in STEPS}
        # One call for 73 steps, and nothing per step.
        redis.get_wrf_layers_bulk.assert_awaited_once_with(PRODUCT, INIT_TAG, STEPS)
        redis.get_wrf_layers.assert_not_awaited()
        s3.list_wrf_layers.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_only_unindexed_steps_reach_s3(self):
        """A run still filling in must not re-discover what is already indexed."""
        redis, s3 = _redis(), _s3(["contours"])
        redis.get_wrf_layers_bulk = AsyncMock(
            return_value={"F001": ["barbs"], "F002": [], "F003": ["barbs"]}
        )
        strategy = WrfFullSyncStrategy(redis, s3, 10, 10, 10)

        result = await strategy.list_layers_bulk(
            PRODUCT, INIT_TAG, ["F001", "F002", "F003"]
        )

        assert result == {
            "F001": ["barbs"],
            "F002": ["contours"],
            "F003": ["barbs"],
        }
        assert s3.list_wrf_layers.await_count == 1
        assert s3.list_wrf_layers.await_args.args == (PRODUCT, INIT_TAG, "F002")

    @pytest.mark.asyncio
    async def test_redis_only_deployment_returns_the_index_verbatim(self):
        """Without an S3 client there is no fallback to delegate misses to."""
        redis = _redis()
        redis.get_wrf_layers_bulk = AsyncMock(
            return_value={"F001": [], "F002": ["barbs"]}
        )
        strategy = WrfFullSyncStrategy(redis)

        result = await strategy.list_layers_bulk(PRODUCT, INIT_TAG, ["F001", "F002"])

        assert result == {"F001": [], "F002": ["barbs"]}

    @pytest.mark.asyncio
    async def test_no_steps_touches_nothing(self):
        redis, s3 = _redis(), _s3()
        strategy = WrfFullSyncStrategy(redis, s3, 10, 10, 10)

        assert await strategy.list_layers_bulk(PRODUCT, INIT_TAG, []) == {}
        s3.list_wrf_layers.assert_not_awaited()


class TestOnDemandBulkListing:
    @pytest.mark.asyncio
    async def test_cold_discovery_is_bounded_not_one_per_step(self):
        """The cold path still fans out, but never wider than the semaphore.

        This is the regression guard: unbounded, 73 steps meant 73 in-flight
        Redis reads around the S3 LISTs, which is what exhausted the pool.
        """
        redis, s3 = _redis(), _s3()
        inflight = 0
        peak = 0

        async def discover(*_args, **_kwargs):
            nonlocal inflight, peak
            inflight += 1
            peak = max(peak, inflight)
            try:
                await asyncio.sleep(0)
                return ["barbs"]
            finally:
                inflight -= 1

        s3.list_wrf_layers = AsyncMock(side_effect=discover)
        strategy = WrfOnDemandStrategy(redis, s3, 10, 10, 10)

        result = await strategy.list_layers_bulk(PRODUCT, INIT_TAG, STEPS)

        assert len(result) == len(STEPS)
        assert result["F073"] == ["barbs"]
        assert peak <= _LAYER_DISCOVERY_CONCURRENCY

    @pytest.mark.asyncio
    async def test_discovered_layers_are_cached_per_step(self):
        redis, s3 = _redis(), _s3()
        strategy = WrfOnDemandStrategy(redis, s3, 10, 10, 30)

        await strategy.list_layers_bulk(PRODUCT, INIT_TAG, ["F001"])

        assert redis.cache_listing.await_args.args[0] == (
            f"cache:listing:wrf:{PRODUCT}:{INIT_TAG}:F001:layers"
        )

    @pytest.mark.asyncio
    async def test_no_steps_touches_nothing(self):
        redis, s3 = _redis(), _s3()
        strategy = WrfOnDemandStrategy(redis, s3, 10, 10, 10)

        assert await strategy.list_layers_bulk(PRODUCT, INIT_TAG, []) == {}
        s3.list_wrf_layers.assert_not_awaited()
