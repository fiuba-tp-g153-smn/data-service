"""Tests that sync/scrape services record per-domain cycle rows into the store."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from clients.metrics_store import MetricsStore
from db.migrate import run_migrations
from services.radar_sync_service import RadarSyncService
from services.satellite_sync_service import SatelliteSyncService
from services.weather_stations_scraper_service import WeatherStationsScraperService
from settings import Settings


@pytest_asyncio.fixture
async def store(tmp_path):
    db_path = tmp_path / "metrics.sqlite"
    run_migrations(db_path)  # schema is Alembic-owned; connect no longer creates it
    s = MetricsStore(str(db_path))
    await s.connect()
    try:
        yield s
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_timed_domain_records_ok_cycle(store):
    svc = SatelliteSyncService()  # _domain == "satellite"
    svc.set_metrics_store(store)

    async def domain_cycle():
        return (7, 0)

    downloaded, errors, timed_out = await svc._timed_domain(domain_cycle())

    assert (downloaded, errors) == (7, 0)
    assert timed_out is False
    rows = await store.get_latest_sync_per_domain()
    assert len(rows) == 1
    assert rows[0].domain == "satellite"
    assert rows[0].downloaded == 7
    assert rows[0].outcome == "ok"
    assert rows[0].duration_ms >= 0


@pytest.mark.asyncio
async def test_timed_domain_marks_error_outcome_when_errors(store):
    svc = RadarSyncService()  # _domain == "radar"
    svc.set_metrics_store(store)

    async def domain_cycle():
        return (2, 3)

    await svc._timed_domain(domain_cycle())

    rows = {r.domain: r for r in await store.get_latest_sync_per_domain()}
    assert rows["radar"].errors == 3
    assert rows["radar"].outcome == "error"


@pytest.mark.asyncio
async def test_timed_domain_records_timeout_outcome(store):
    """A cycle that overruns the watchdog is recorded as a timeout, not raised."""
    svc = SatelliteSyncService()
    svc.set_metrics_store(store)
    svc._timeout = 0.01  # force the watchdog to fire

    async def slow_cycle():
        await asyncio.sleep(5)
        return (1, 0)

    downloaded, errors, timed_out = await svc._timed_domain(slow_cycle())

    # A timeout preempts a resumable cycle: recorded with errors=0 and flagged
    # via timed_out, distinguished only by the "timeout" outcome — never counted
    # as a hard error.
    assert (downloaded, errors) == (0, 0)
    assert timed_out is True
    rows = {r.domain: r for r in await store.get_latest_sync_per_domain()}
    assert rows["satellite"].outcome == "timeout"
    assert rows["satellite"].errors == 0


@pytest.mark.asyncio
async def test_record_cycle_is_noop_without_store():
    svc = SatelliteSyncService()  # no metrics store configured
    # Must not raise when the store is absent.
    await svc._record_cycle("a", "b", 1, 1, 0)


@pytest.mark.asyncio
async def test_weather_scraper_records_cycle(store):
    svc = WeatherStationsScraperService(
        settings=Settings.get_settings(),
        s3_client=AsyncMock(),
        smn_client=AsyncMock(),
        registry_client=AsyncMock(),
        redis_client=None,
        metrics_store=store,
    )

    await svc._record_cycle(datetime.now(timezone.utc), downloaded=410, errors=0)

    rows = {r.domain: r for r in await store.get_latest_sync_per_domain()}
    assert rows["weather_stations"].downloaded == 410
    assert rows["weather_stations"].outcome == "ok"
