"""How `BasemapScraperService` behaves when its storage backends go down.

The scraper's failure mode before the storage circuit existed: with S3
unreachable it kept pulling every tile in the bounding box from the upstream
provider, discarded each one, queued a SQLite row for it and logged a warning —
thousands of times per sweep. These tests pin the behaviour that replaced it.
"""

import logging
from dataclasses import replace

import pytest
import pytest_asyncio
from botocore.exceptions import EndpointConnectionError

from clients.basemap_state_store import BasemapStateStore
from services.basemap_config import count_tiles
from services.storage_circuit import StorageCircuit
from tests.test_basemap_scraper_resume import (
    FakeHttp,
    _make_bbox,
    _make_provider,
    _make_scraper,
)

# Fast backoff so the pausing logic is exercised without the suite sleeping
# through a real 1→2→4→8→16→30s schedule.
_FAST_CIRCUIT = {
    "window": 10,
    "min_samples": 3,
    "threshold": 0.8,
    "base_cooldown": 0.001,
    "max_cooldown": 0.002,
    "max_consecutive_trips": 3,
}


@pytest_asyncio.fixture
async def store(tmp_path):
    state = BasemapStateStore(str(tmp_path / "state.sqlite"))
    await state.connect()
    try:
        yield state
    finally:
        await state.close()


def _s3_outage() -> EndpointConnectionError:
    return EndpointConnectionError(endpoint_url="http://host.docker.internal:9000")


def _build(store_, http, provider, bbox, **overrides):
    """Scraper with both storage circuits swapped for fast-backoff ones."""
    scraper = _make_scraper(
        store_,
        http,
        provider,
        bbox,
        basemap_scrape_fanout_window=2,
        # The shared helper caps zooms at 6; these tests need real tile
        # volume (z8=80, z9=285) for the gate to have anything to gate.
        basemap_cache_max_zoom=9,
        **overrides,
    )
    scraper._s3_circuit = StorageCircuit("S3", **_FAST_CIRCUIT)
    scraper._redis_circuit = StorageCircuit("Redis", **_FAST_CIRCUIT)
    return scraper


@pytest.mark.asyncio
async def test_s3_outage_stops_pulling_tiles_from_the_provider(store):
    """The point of the circuit: stop spending upstream quota on tiles we
    already know we cannot persist."""
    provider = _make_provider(min_zoom=8, max_zoom=9)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _build(store, http, provider, bbox)
    scraper._s3.upload_tile.side_effect = _s3_outage()

    await scraper._run_sync()

    total = count_tiles(8, bbox) + count_tiles(9, bbox)
    assert len(http.calls) < total, "circuit never gated the upstream fetches"


@pytest.mark.asyncio
async def test_s3_outage_does_not_queue_a_row_per_skipped_tile(store):
    """Skipped tiles were never attempted — queueing them would write one
    SQLite row per tile for the whole outage."""
    provider = _make_provider(min_zoom=8, max_zoom=9)
    bbox = _make_bbox()
    scraper = _build(store, FakeHttp(), provider, bbox)
    scraper._s3.upload_tile.side_effect = _s3_outage()

    await scraper._run_sync()

    queued = 0
    for zoom in (8, 9):
        queued += len(await store.list_failed(provider.provider_id, zoom))
    total = count_tiles(8, bbox) + count_tiles(9, bbox)
    assert queued < total


@pytest.mark.asyncio
async def test_abandoned_sweep_preserves_cursor_for_resume(store):
    """A sweep that gives up mid-flight must resume where it stopped, not
    restart a multi-day scrape from zero."""
    provider = _make_provider(min_zoom=8, max_zoom=9)
    bbox = _make_bbox()
    scraper = _build(store, FakeHttp(), provider, bbox)
    scraper._s3.upload_tile.side_effect = _s3_outage()

    await scraper._run_sync()

    assert await store.get_cursor(provider.provider_id) is not None
    # And the sweep is never counted as complete, so the next cycle retries.
    assert await store.get_last_completed(provider.provider_id) is None
    assert scraper._storage_retry_due is True


@pytest.mark.asyncio
async def test_s3_outage_logs_a_handful_of_lines_not_one_per_tile(store, caplog):
    """The symptom that started this: a WARNING per tile."""
    provider = _make_provider(min_zoom=8, max_zoom=9)
    bbox = _make_bbox()
    scraper = _build(store, FakeHttp(), provider, bbox)
    scraper._s3.upload_tile.side_effect = _s3_outage()

    with caplog.at_level(logging.WARNING, logger="services.basemap_scraper_service"):
        await scraper._run_sync()

    storage_warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "S3" in r.getMessage()
    ]
    assert 0 < len(storage_warnings) <= 6, [r.getMessage() for r in storage_warnings]


@pytest.mark.asyncio
async def test_redis_outage_does_not_block_the_durable_backup(store):
    """S3 is what the sweep exists to produce. A cold hot-cache repopulates on
    first read, so it must not mark the sweep incomplete."""
    provider = _make_provider(min_zoom=8, max_zoom=8)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _build(store, http, provider, bbox)
    scraper._redis.store_basemap_tile.side_effect = OSError("redis down")

    await scraper._run_sync()

    assert await store.get_last_completed(provider.provider_id) is not None
    assert await store.get_cursor(provider.provider_id) is None
    assert scraper._storage_retry_due is False
    # Every tile still reached S3.
    assert scraper._s3.upload_tile.await_count == count_tiles(8, bbox)


@pytest.mark.asyncio
async def test_sweep_resumes_at_full_speed_once_s3_recovers(store):
    """A blip should cost seconds of pausing, not the sweep."""
    provider = _make_provider(min_zoom=8, max_zoom=8)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _build(store, http, provider, bbox)

    failures = {"left": 4}

    async def flaky_upload(*_args, **_kwargs):
        if failures["left"] > 0:
            failures["left"] -= 1
            raise _s3_outage()

    scraper._s3.upload_tile.side_effect = flaky_upload

    await scraper._run_sync()

    # Ran to the end rather than abandoning: every tile was fetched and the
    # cursor was cleared. (last_completed still isn't stamped — the sweep did
    # hit storage errors, and the pre-existing rule defers the stamp so the
    # four queued tiles get retried on the next cycle.)
    total = count_tiles(8, bbox)
    assert await store.get_cursor(provider.provider_id) is None
    assert failures["left"] == 0, "S3 never got used again after recovery"
    # Only the handful gated during the brief open window were skipped; the
    # sweep carried on through the rest at full speed.
    assert total - len(http.calls) <= 4
    assert scraper._s3_circuit.is_open() is False


@pytest.mark.asyncio
async def test_remaining_providers_skip_once_storage_is_known_down(store):
    """Storage is shared: the second provider shouldn't re-derive the outage
    through its own backoff schedule."""
    first = _make_provider(min_zoom=8, max_zoom=8)
    second = replace(_make_provider(min_zoom=8, max_zoom=8), provider_id="fake2")
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _build(
        store,
        http,
        first,
        bbox,
        providers={first.provider_id: first, second.provider_id: second},
    )
    scraper._s3.upload_tile.side_effect = _s3_outage()

    await scraper._run_sync()

    assert scraper._storage_down_this_cycle is True
    # The second provider never started, so it left no resume state behind.
    assert await store.get_cursor(second.provider_id) is None
