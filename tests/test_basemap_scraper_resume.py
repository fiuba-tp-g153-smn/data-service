"""Unit tests for resumable-scrape wiring in `BasemapScraperService`."""

import asyncio
import time
from types import SimpleNamespace
from typing import Dict, Iterable, Set, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from clients.basemap_state_store import BasemapStateStore, Cursor
from clients.http_tile_client import ProviderUnavailableError
from services.basemap_config import BasemapProvider, BoundingBox, count_tiles
from services.basemap_scraper_service import BasemapScraperService


def _make_provider(min_zoom: int = 5, max_zoom: int = 6) -> BasemapProvider:
    return BasemapProvider(
        provider_id="fake",
        name="Fake",
        source_url_template="https://example.test/{z}/{x}/{y}.png",
        is_tms=False,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        cache_max_zoom=max_zoom,
        attribution="",
    )


def _make_bbox() -> BoundingBox:
    # Bbox large enough to yield >=3 tiles at z=5 and multiple tiles at z=6.
    return BoundingBox(lat_min=-40.0, lat_max=-30.0, lon_min=-65.0, lon_max=-55.0)


def _make_settings(**overrides) -> SimpleNamespace:
    """Mimic `Settings` attrs the scraper reads."""
    base = {
        "basemap_scrape_interval_seconds": 1,
        "basemap_cache_max_zoom": 6,
        "basemap_scrape_lock_path": "/tmp/test_basemap_scrape.lock",
        "basemap_scrape_checkpoint_every": 1,
        "basemap_scrape_checkpoint_seconds": 0.001,
        # Large so existing tests run as a single chunk (chunking is exercised
        # explicitly by the dedicated fan-out tests below).
        "basemap_scrape_fanout_window": 10_000,
        # Rate-based circuit breaker. Lenient defaults so ordinary tests never
        # trip on a stray failure; breaker tests provoke trips with all-unavailable
        # sweeps (z=5 yields 4 tiles, so min_samples=3 still trips). schedule keeps
        # the first cooldown short enough to exercise.
        "basemap_provider_error_rate_threshold": 0.95,
        "basemap_provider_error_rate_min_samples": 3,
        "basemap_provider_cooldown_schedule": [300, 900, 3600, 10800, 21600],
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class FakeHttp:
    """Stand-in for `HttpTileClient.download_tile` with scripted outcomes.

    Three per-tile dispositions:
      * default → returns bytes (OK)
      * listed in ``fail_tiles``     → returns ``None`` (MISSING, e.g. 404)
      * listed in ``unavailable_tiles`` → raises ``ProviderUnavailableError``
    """

    def __init__(
        self,
        fail_tiles: Iterable[Tuple[int, int, int]] = (),
        unavailable_tiles: Iterable[Tuple[int, int, int]] = (),
        all_unavailable: bool = False,
    ):
        self._fail_tiles: Set[Tuple[int, int, int]] = set(fail_tiles)
        self._unavailable_tiles: Set[Tuple[int, int, int]] = set(unavailable_tiles)
        self._all_unavailable = all_unavailable
        self.calls: list[str] = []

    async def download_tile(self, url: str):
        self.calls.append(url)
        parts = url.rstrip(".png").split("/")
        z, x, y = int(parts[-3]), int(parts[-2]), int(parts[-1])
        if self._all_unavailable or (z, x, y) in self._unavailable_tiles:
            raise ProviderUnavailableError(url, "scripted outage")
        if (z, x, y) in self._fail_tiles:
            return None
        return b"fake-bytes"


@pytest_asyncio.fixture
async def store(tmp_path):
    s = BasemapStateStore(str(tmp_path / "state.sqlite"))
    await s.connect()
    try:
        yield s
    finally:
        await s.close()


def _make_scraper(
    store_: BasemapStateStore,
    http: FakeHttp,
    provider: BasemapProvider,
    bbox: BoundingBox,
    *,
    redis_writes_enabled: bool = True,
    parallelism_mode: str = "sequential",
    providers: Dict[str, BasemapProvider] | None = None,
    **settings_overrides,
) -> BasemapScraperService:
    settings = _make_settings(**settings_overrides)
    s3 = MagicMock()
    s3.upload_tile = AsyncMock()
    redis = MagicMock()
    redis.store_basemap_tile = AsyncMock()
    redis.clear_basemap_tile_miss = AsyncMock()
    if providers is None:
        providers = {provider.provider_id: provider}
    # Tests default the lifecycle call to pre-applied so they don't have to
    # mock the S3 API surface unless they're specifically asserting the
    # retry logic.
    s3.ensure_lifecycle_expiration = AsyncMock(return_value=True)
    return BasemapScraperService(
        settings=settings,  # type: ignore[arg-type]
        s3_client=s3,
        redis_client=redis,
        http_client=http,  # type: ignore[arg-type]
        state_store=store_,
        providers=providers,
        bbox=bbox,
        tile_ttl=60,
        s3_object_ttl_days=35,
        redis_writes_enabled=redis_writes_enabled,
        parallelism_mode=parallelism_mode,
    )


@pytest.mark.asyncio
async def test_successful_sweep_clears_all_state(store):
    """A provider scraped end-to-end leaves zero rows in cursor + failed tables."""
    provider = _make_provider(min_zoom=5, max_zoom=6)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)

    await scraper._run_sync()  # pylint: disable=protected-access

    assert await store.get_cursor(provider.provider_id) is None
    for zoom in (5, 6):
        assert await store.list_failed(provider.provider_id, zoom) == []

    # Every tile in bbox for every zoom got downloaded.
    expected = count_tiles(5, bbox) + count_tiles(6, bbox)
    assert len(http.calls) == expected


@pytest.mark.asyncio
async def test_failed_tiles_are_recorded_and_retried(store):
    """A tile that fails in sweep 1 is queued and succeeds on the next cycle's retry."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()

    # Pick one real tile in the bbox to fail on the first pass.
    from services.basemap_config import (
        iter_tiles,
    )  # local import to avoid cycle in fixture

    coords = list(iter_tiles(5, bbox))
    fail_zxy = coords[0]

    http_fail = FakeHttp(fail_tiles=[fail_zxy])
    scraper = _make_scraper(store, http_fail, provider, bbox)
    await scraper._run_sync()  # pylint: disable=protected-access

    # Interrupted cycle would have rows; but since *all* tiles processed (just one failed),
    # the cursor is cleared by end-of-provider, and failed row is also cleared:
    # end-of-provider always calls clear_failed_for_provider.
    # So after a complete sweep we expect NO failed rows either.
    assert await store.list_failed(provider.provider_id, 5) == []

    # To validate retry mechanics directly, use a fresh store: seed a failed row,
    # run with a non-failing http, and confirm it gets retried and removed.
    from clients.basemap_state_store import BasemapStateStore as _Store  # noqa: WPS433

    fresh = _Store(store._db_path + ".retry")  # pylint: disable=protected-access
    await fresh.connect()
    try:
        _z, _x, _y = fail_zxy
        await fresh.add_failed(provider.provider_id, _z, _x, _y)
        # Seed cursor past this zoom so main sweep is a no-op; retry phase should
        # still fire at z=5 and clear the failed row.
        # But the scrape starts at cursor.zoom — set cursor to z=5 index=total so
        # main sweep is no-op, but retry still runs.
        total = len(coords)
        await fresh.set_cursor(provider.provider_id, 5, total)

        http_ok = FakeHttp()
        scraper2 = _make_scraper(fresh, http_ok, provider, bbox)
        await scraper2._run_sync()  # pylint: disable=protected-access

        assert await fresh.list_failed(provider.provider_id, 5) == []
        assert await fresh.get_cursor(provider.provider_id) is None
    finally:
        await fresh.close()


@pytest.mark.asyncio
async def test_resume_from_midzoom_cursor_skips_already_done(store):
    """If cursor says (zoom=5, index=N), the sweep only covers coords[N:] for z=5."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()

    from services.basemap_config import iter_tiles

    coords = list(iter_tiles(5, bbox))
    assert len(coords) >= 3, "bbox too small for this test"

    await store.set_cursor(provider.provider_id, 5, 2)

    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)
    await scraper._run_sync()  # pylint: disable=protected-access

    # Sweep called only for indices >= 2.
    assert len(http.calls) == len(coords) - 2

    # Fully completed: cursor cleared.
    assert await store.get_cursor(provider.provider_id) is None


@pytest.mark.asyncio
async def test_resume_past_zoom_skips_early_zooms(store):
    """If cursor says zoom=6, z=5 is skipped entirely on resume."""
    provider = _make_provider(min_zoom=5, max_zoom=6)
    bbox = _make_bbox()

    from services.basemap_config import iter_tiles

    await store.set_cursor(provider.provider_id, 6, 0)

    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)
    await scraper._run_sync()  # pylint: disable=protected-access

    # Only z=6 tiles were fetched.
    expected = count_tiles(6, bbox)
    assert len(http.calls) == expected
    assert all("/6/" in url for url in http.calls)

    # Clean completion.
    assert await store.get_cursor(provider.provider_id) is None


@pytest.mark.asyncio
async def test_watermark_checkpoint_persists_during_sweep(store):
    """With checkpoint_every=1, cursor is flushed after each tile completion."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()

    observed: list[Cursor] = []
    real_set_cursor = store.set_cursor

    async def spy(provider_id, zoom, tile_index):
        observed.append(Cursor(zoom=zoom, tile_index=tile_index))
        await real_set_cursor(provider_id, zoom, tile_index)

    store.set_cursor = spy  # type: ignore[assignment]

    http = FakeHttp()
    scraper = _make_scraper(
        store,
        http,
        provider,
        bbox,
        basemap_scrape_checkpoint_every=1,
        basemap_scrape_checkpoint_seconds=0.001,
    )
    await scraper._run_sync()  # pylint: disable=protected-access

    # Watermark is monotonically non-decreasing.
    indices = [c.tile_index for c in observed if c.zoom == 5]
    assert indices == sorted(indices)
    # Reaches total tile count before zoom advance.
    from services.basemap_config import iter_tiles

    total = len(list(iter_tiles(5, bbox)))
    assert max(indices) == total


@pytest.mark.asyncio
async def test_scraper_clears_negative_cache_after_upload(store):
    """Every successful tile upload must invalidate the reader's miss tombstone."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)

    await scraper._run_sync()  # pylint: disable=protected-access

    clear_mock = (
        scraper._redis.clear_basemap_tile_miss
    )  # pylint: disable=protected-access
    from services.basemap_config import iter_tiles

    expected_calls = {
        (provider.provider_id, z, x, y) for (z, x, y) in iter_tiles(5, bbox)
    }
    actual_calls = {call.args for call in clear_mock.call_args_list}
    assert actual_calls == expected_calls


@pytest.mark.asyncio
async def test_redis_writes_disabled_skips_all_scraper_redis_calls(store):
    """no_cache mode: scraper uploads to S3 but never touches Redis."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox, redis_writes_enabled=False)

    await scraper._run_sync()  # pylint: disable=protected-access

    # pylint: disable=protected-access
    s3_mock = scraper._s3.upload_tile
    redis_store_mock = scraper._redis.store_basemap_tile
    redis_clear_mock = scraper._redis.clear_basemap_tile_miss

    # S3 uploads happened (one per tile), but Redis was untouched.
    assert s3_mock.await_count > 0
    redis_store_mock.assert_not_called()
    redis_clear_mock.assert_not_called()


@pytest.mark.asyncio
async def test_successful_sweep_persists_last_completed(store):
    """After a full sweep, `basemap_scrape_last_completed` holds a recent timestamp."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)

    before = int(time.time())
    await scraper._run_sync()  # pylint: disable=protected-access
    after = int(time.time())

    stamped = await store.get_last_completed(provider.provider_id)
    assert stamped is not None
    assert before <= stamped <= after


@pytest.mark.asyncio
async def test_cooldown_skips_provider_within_window(store, monkeypatch):
    """A provider completed < interval ago is skipped on the next `_run_sync`."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(
        store, http, provider, bbox, basemap_scrape_interval_seconds=604800
    )

    # Stamp completion 1 hour ago; still inside the 7-day window.
    fake_now = 2_000_000_000
    await store.set_last_completed(provider.provider_id, fake_now - 3600)
    monkeypatch.setattr("services.basemap_scraper_service.time.time", lambda: fake_now)

    await scraper._run_sync()  # pylint: disable=protected-access

    # No tile fetched; cursor stays absent; completion stamp unchanged.
    assert http.calls == []
    assert await store.get_cursor(provider.provider_id) is None
    assert await store.get_last_completed(provider.provider_id) == fake_now - 3600


@pytest.mark.asyncio
async def test_cooldown_ignored_when_cursor_present(store, monkeypatch):
    """A live resume cursor wins over the cool-down — the sweep still runs."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(
        store, http, provider, bbox, basemap_scrape_interval_seconds=604800
    )

    fake_now = 2_000_000_000
    await store.set_last_completed(provider.provider_id, fake_now - 60)
    await store.set_cursor(provider.provider_id, 5, 0)
    monkeypatch.setattr("services.basemap_scraper_service.time.time", lambda: fake_now)

    await scraper._run_sync()  # pylint: disable=protected-access

    # Sweep actually executed: http received calls and completion got restamped.
    assert len(http.calls) > 0
    assert await store.get_last_completed(provider.provider_id) == fake_now


@pytest.mark.asyncio
async def test_cooldown_expired_runs_full_sweep(store, monkeypatch):
    """Once the cool-down expires, the next `_run_sync` scrapes normally."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(
        store, http, provider, bbox, basemap_scrape_interval_seconds=604800
    )

    fake_now = 2_000_000_000
    await store.set_last_completed(provider.provider_id, fake_now - 604801)
    monkeypatch.setattr("services.basemap_scraper_service.time.time", lambda: fake_now)

    await scraper._run_sync()  # pylint: disable=protected-access

    assert len(http.calls) > 0
    assert await store.get_last_completed(provider.provider_id) == fake_now


@pytest.mark.asyncio
async def test_compute_next_sleep_returns_soonest_remaining(store, monkeypatch):
    """`_compute_next_sleep` reflects the soonest-due provider, floored at 60s."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(
        store, http, provider, bbox, basemap_scrape_interval_seconds=604800
    )

    fake_now = 2_000_000_000
    # 100k seconds remaining: last = now - (604800 - 100000).
    await store.set_last_completed(provider.provider_id, fake_now - (604800 - 100000))
    monkeypatch.setattr("services.basemap_scraper_service.time.time", lambda: fake_now)

    got = await scraper._compute_next_sleep(
        default=604800.0
    )  # pylint: disable=protected-access
    assert got == 100000.0


@pytest.mark.asyncio
async def test_compute_next_sleep_zero_when_cursor_present(store):
    """A cursor forces immediate re-run (sleep == 0)."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)

    await scraper._state.set_cursor(
        provider.provider_id, 5, 0
    )  # pylint: disable=protected-access
    got = await scraper._compute_next_sleep(
        default=9999.0
    )  # pylint: disable=protected-access
    assert got == 0.0


@pytest.mark.asyncio
async def test_compute_next_sleep_floored_at_60s(store, monkeypatch):
    """When the soonest-due remaining is tiny, sleep is floored to 60s."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(
        store, http, provider, bbox, basemap_scrape_interval_seconds=604800
    )

    fake_now = 2_000_000_000
    # 5 seconds remaining — below the 60s floor.
    await store.set_last_completed(provider.provider_id, fake_now - (604800 - 5))
    monkeypatch.setattr("services.basemap_scraper_service.time.time", lambda: fake_now)

    got = await scraper._compute_next_sleep(
        default=604800.0
    )  # pylint: disable=protected-access
    assert got == 60.0


@pytest.mark.asyncio
async def test_failed_tile_recorded_in_store(store):
    """A download failure during the main sweep is written to `basemap_scrape_failed`."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()

    from services.basemap_config import iter_tiles

    coords = list(iter_tiles(5, bbox))
    bad = coords[1]  # fail the second tile so watermark is tested too

    # Capture add_failed calls directly.
    seen: list[tuple[str, int, int, int]] = []
    real_add = store.add_failed

    async def spy(provider_id, zoom, x, y):
        seen.append((provider_id, zoom, x, y))
        await real_add(provider_id, zoom, x, y)

    store.add_failed = spy  # type: ignore[assignment]

    http = FakeHttp(fail_tiles=[bad])
    scraper = _make_scraper(store, http, provider, bbox)
    await scraper._run_sync()  # pylint: disable=protected-access

    # The failed tile was recorded (before end-of-provider cleanup wipes it).
    assert (provider.provider_id, bad[0], bad[1], bad[2]) in seen


# --------------------------------------------------------------------------- #
# Parallelism mode dispatcher
# --------------------------------------------------------------------------- #


def _provider_with_url(provider_id: str, url: str) -> BasemapProvider:
    return BasemapProvider(
        provider_id=provider_id,
        name=provider_id,
        source_url_template=url,
        is_tms=False,
        min_zoom=5,
        max_zoom=5,
        cache_max_zoom=5,
        attribution="",
    )


def test_build_scrape_groups_sequential(store):
    """sequential → one group containing every provider in order."""
    providers = {
        "a": _provider_with_url("a", "https://host-a.test/{z}/{x}/{y}.png"),
        "b": _provider_with_url("b", "https://host-b.test/{z}/{x}/{y}.png"),
    }
    scraper = _make_scraper(
        store,
        FakeHttp(),
        providers["a"],
        _make_bbox(),
        parallelism_mode="sequential",
        providers=providers,
    )
    groups = scraper._build_scrape_groups()  # pylint: disable=protected-access
    assert [[p.provider_id for p in g] for g in groups] == [["a", "b"]]


def test_build_scrape_groups_full(store):
    """full → each provider becomes its own singleton group."""
    providers = {
        "a": _provider_with_url("a", "https://host-a.test/{z}/{x}/{y}.png"),
        "b": _provider_with_url("b", "https://host-b.test/{z}/{x}/{y}.png"),
        "c": _provider_with_url("c", "https://host-a.test/{z}/{x}/{y}.png"),
    }
    scraper = _make_scraper(
        store,
        FakeHttp(),
        providers["a"],
        _make_bbox(),
        parallelism_mode="full",
        providers=providers,
    )
    groups = scraper._build_scrape_groups()  # pylint: disable=protected-access
    assert sorted([p.provider_id for g in groups for p in g]) == ["a", "b", "c"]
    assert all(len(g) == 1 for g in groups)


def test_build_scrape_groups_per_origin(store):
    """per_origin → providers sharing a host collapse into one group."""
    providers = {
        "argenmap": _provider_with_url(
            "argenmap", "https://wms.ign.gob.ar/geoserver/{z}/{x}/{y}.png"
        ),
        "argenmapGris": _provider_with_url(
            "argenmapGris", "https://wms.ign.gob.ar/gris/{z}/{x}/{y}.png"
        ),
        "satellite": _provider_with_url(
            "satellite", "https://server.arcgisonline.com/sat/{z}/{x}/{y}.png"
        ),
        "google": _provider_with_url(
            "google", "https://mt1.google.com/vt/{z}/{x}/{y}.png"
        ),
    }
    scraper = _make_scraper(
        store,
        FakeHttp(),
        providers["argenmap"],
        _make_bbox(),
        parallelism_mode="per_origin",
        providers=providers,
    )
    groups = scraper._build_scrape_groups()  # pylint: disable=protected-access
    membership = sorted(sorted(p.provider_id for p in g) for g in groups)
    assert membership == [
        ["argenmap", "argenmapGris"],
        ["google"],
        ["satellite"],
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["sequential", "per_origin", "full"])
async def test_run_sync_end_to_end_across_modes(store, mode):
    """Every tile is fetched once regardless of parallelism mode."""
    providers = {
        "argenmap": _provider_with_url(
            "argenmap", "https://wms.ign.gob.ar/{z}/{x}/{y}.png"
        ),
        "satellite": _provider_with_url(
            "satellite", "https://server.arcgisonline.com/{z}/{x}/{y}.png"
        ),
    }
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(
        store,
        http,
        providers["argenmap"],
        bbox,
        parallelism_mode=mode,
        providers=providers,
    )

    await scraper._run_sync()  # pylint: disable=protected-access

    expected_per_provider = count_tiles(5, bbox)
    assert len(http.calls) == expected_per_provider * len(providers)
    for pid in providers:
        assert await store.get_cursor(pid) is None
        assert await store.list_failed(pid, 5) == []
        assert await store.get_last_completed(pid) is not None


# --------------------------------------------------------------------------- #
# Circuit breaker (provider health)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_circuit_opens_when_error_rate_exceeds_threshold(store):
    """An all-unavailable sweep (100% error rate) trips the provider; state persisted."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp(all_unavailable=True)
    scraper = _make_scraper(store, http, provider, bbox)

    await scraper._run_sync()  # pylint: disable=protected-access

    health = await store.get_health(provider.provider_id)
    assert health is not None
    assert health.consecutive_trips == 1
    assert health.cooldown_until > int(time.time())
    assert "tasa de error" in health.last_reason.lower()

    # Cursor preserved so the next (post-cooldown) cycle resumes.
    assert await store.get_cursor(provider.provider_id) is not None
    # last_completed NOT stamped — the sweep didn't actually finish.
    assert await store.get_last_completed(provider.provider_id) is None


@pytest.mark.asyncio
async def test_provider_in_cooldown_is_skipped(store):
    """Active cooldown → zero HTTP calls for that provider this cycle."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    future = int(time.time()) + 3600
    await store.open_circuit(
        provider.provider_id,
        consecutive_trips=2,
        cooldown_until=future,
        reason="seeded",
    )

    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)
    await scraper._run_sync()  # pylint: disable=protected-access

    assert http.calls == []
    # Health row untouched.
    health = await store.get_health(provider.provider_id)
    assert health is not None
    assert health.cooldown_until == future


@pytest.mark.asyncio
async def test_expired_cooldown_allows_retry_and_clean_finish(store):
    """Once cooldown elapses and the sweep finishes cleanly, the circuit closes."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    # Seed an expired cooldown.
    await store.open_circuit(
        provider.provider_id,
        consecutive_trips=1,
        cooldown_until=int(time.time()) - 1,
        reason="seeded-expired",
    )

    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)
    await scraper._run_sync()  # pylint: disable=protected-access

    # Sweep actually ran, and on clean finish the health row is cleared.
    assert len(http.calls) > 0
    assert await store.get_health(provider.provider_id) is None
    assert await store.get_last_completed(provider.provider_id) is not None


def test_rate_breaker_trips_after_min_samples(store):
    """Direct unit test: trips only past min_samples AND above the rate threshold."""
    # pylint: disable=protected-access
    from services.basemap_scraper_service import (  # local import to avoid clutter
        _ProviderSweepState,
        _TileOutcome,
    )

    scraper = _make_scraper(
        store,
        FakeHttp(),
        _make_provider(),
        _make_bbox(),
        basemap_provider_error_rate_threshold=0.5,
        basemap_provider_error_rate_min_samples=4,
    )
    state = _ProviderSweepState()

    # 3 UNAVAILABLE — 100% error rate but below min_samples (4): no trip yet.
    for i in range(3):
        scraper._update_sweep_state(state, _TileOutcome.UNAVAILABLE, 5, 0, i)
    assert state.attempted == 3
    assert state.failed == 3
    assert not state.tripped

    # MISSING is neutral — excluded from attempted and failed.
    scraper._update_sweep_state(state, _TileOutcome.MISSING, 5, 0, 99)
    assert state.attempted == 3

    # A 4th fetch reaches min_samples; rate (3/4 = 75% > 50%) trips the circuit.
    scraper._update_sweep_state(state, _TileOutcome.OK, 5, 0, 100)
    assert state.attempted == 4
    assert state.tripped
    assert "tasa de error" in state.last_reason


def test_low_error_rate_does_not_trip(store):
    """A handful of failures among many OK fetches stays under the threshold."""
    # pylint: disable=protected-access
    from services.basemap_scraper_service import (
        _ProviderSweepState,
        _TileOutcome,
    )

    scraper = _make_scraper(
        store,
        FakeHttp(),
        _make_provider(),
        _make_bbox(),
        basemap_provider_error_rate_threshold=0.05,
        basemap_provider_error_rate_min_samples=10,
    )
    state = _ProviderSweepState()

    # 198 OK + 2 UNAVAILABLE = 1% error rate, well under the 5% threshold.
    for i in range(198):
        scraper._update_sweep_state(state, _TileOutcome.OK, 5, 0, i)
    scraper._update_sweep_state(state, _TileOutcome.UNAVAILABLE, 5, 1, 0)
    scraper._update_sweep_state(state, _TileOutcome.UNAVAILABLE, 5, 1, 1)

    assert state.attempted == 200
    assert state.failed == 2
    assert not state.tripped


@pytest.mark.asyncio
async def test_missing_tiles_do_not_trip_circuit(store):
    """404-style misses (return None) never count as unhealthy — only exceptions do."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    from services.basemap_config import iter_tiles

    coords = list(iter_tiles(5, bbox))
    # Make the first ten tiles miss as 404s (MISSING, not UNAVAILABLE).
    http = FakeHttp(fail_tiles=coords[:10])
    scraper = _make_scraper(store, http, provider, bbox)

    await scraper._run_sync()  # pylint: disable=protected-access

    # No trip despite many misses — sweep completed cleanly.
    assert await store.get_health(provider.provider_id) is None
    assert await store.get_last_completed(provider.provider_id) is not None


@pytest.mark.asyncio
async def test_exponential_backoff_schedule_escalates(store):
    """A second trip uses schedule[1], not schedule[0]."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp(all_unavailable=True)
    scraper = _make_scraper(
        store,
        http,
        provider,
        bbox,
        basemap_provider_cooldown_schedule=[60, 600, 3600],
    )

    # Pretend a prior trip already happened.
    now = int(time.time())
    await store.open_circuit(
        provider.provider_id,
        consecutive_trips=1,
        cooldown_until=now - 1,  # expired so the sweep runs now
        reason="prior",
    )

    await scraper._run_sync()  # pylint: disable=protected-access

    health = await store.get_health(provider.provider_id)
    assert health is not None
    assert health.consecutive_trips == 2
    # Second cooldown must be at least schedule[1] = 600 seconds away.
    assert health.cooldown_until - int(time.time()) >= 590


@pytest.mark.asyncio
async def test_tripped_provider_does_not_block_healthy_peers(store):
    """In per_origin parallelism, a tripped provider doesn't hold up others."""
    providers = {
        "bad": _provider_with_url("bad", "https://bad-host.test/{z}/{x}/{y}.png"),
        "good": _provider_with_url("good", "https://good-host.test/{z}/{x}/{y}.png"),
    }
    bbox = _make_bbox()

    class _MixedHttp(FakeHttp):
        async def download_tile(self, url: str):
            self.calls.append(url)
            if "bad-host" in url:
                raise ProviderUnavailableError(url, "dead host")
            parts = url.rstrip(".png").split("/")
            _ = parts  # keep structure identical
            return b"fake-bytes"

    http = _MixedHttp()
    scraper = _make_scraper(
        store,
        http,
        providers["bad"],
        bbox,
        parallelism_mode="per_origin",
        providers=providers,
    )

    await scraper._run_sync()  # pylint: disable=protected-access

    # Bad provider tripped, good provider completed cleanly.
    assert await store.get_health("bad") is not None
    assert await store.get_health("good") is None
    assert await store.get_last_completed("good") is not None
    assert await store.get_last_completed("bad") is None


# --------------------------------------------------------------------------- #
# Downstream (S3/Redis) storage recovery
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_storage_failures_skip_last_completed_stamp(store):
    """An entirely-failed S3 upload pass must not stamp last_completed."""
    from botocore.exceptions import EndpointConnectionError

    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()  # HTTP fetches succeed; the scraper gets bytes for every tile.
    scraper = _make_scraper(store, http, provider, bbox)
    # Simulate S3 unreachable at upload time.
    scraper._s3.upload_tile = AsyncMock(  # pylint: disable=protected-access
        side_effect=EndpointConnectionError(endpoint_url="http://unreachable:9000")
    )

    await scraper._run_sync()  # pylint: disable=protected-access

    # last_completed NOT stamped — sweep wasn't really successful.
    assert await store.get_last_completed(provider.provider_id) is None
    # Cursor cleared so the next cycle re-sweeps from scratch.
    assert await store.get_cursor(provider.provider_id) is None
    # Storage-retry flag raised so _compute_next_sleep returns ~60s.
    # pylint: disable=protected-access
    assert scraper._storage_retry_due is True


@pytest.mark.asyncio
async def test_compute_next_sleep_floors_to_60s_on_storage_retry(store):
    """With _storage_retry_due set, next sleep is the short-floor regardless of last_completed."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)

    # Pretend a previous cycle just hit storage errors.
    scraper._storage_retry_due = True  # pylint: disable=protected-access

    # Also seed last_completed in the recent past so the non-storage path
    # would otherwise return a large number — proving the floor wins.
    await store.set_last_completed(provider.provider_id, int(time.time()))

    got = await scraper._compute_next_sleep(  # pylint: disable=protected-access
        default=604800.0
    )
    assert got == 60.0


@pytest.mark.asyncio
async def test_storage_recovery_resumes_normal_cadence(store):
    """Once storage comes back, a clean sweep stamps completion and floors go away."""
    from botocore.exceptions import EndpointConnectionError

    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)

    # Cycle 1: storage down.
    scraper._s3.upload_tile = AsyncMock(  # pylint: disable=protected-access
        side_effect=EndpointConnectionError(endpoint_url="http://down:9000")
    )
    await scraper._run_sync()  # pylint: disable=protected-access
    assert await store.get_last_completed(provider.provider_id) is None
    assert scraper._storage_retry_due is True  # pylint: disable=protected-access

    # Cycle 2: storage back up.
    scraper._s3.upload_tile = AsyncMock(
        return_value=None
    )  # pylint: disable=protected-access
    await scraper._run_sync()  # pylint: disable=protected-access
    assert await store.get_last_completed(provider.provider_id) is not None
    # Flag reset at top of _run_sync and never re-raised because no errors.
    assert scraper._storage_retry_due is False  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_storage_error_preserves_failed_queue(store):
    """Failed tiles stay in the retry queue across a storage-error sweep."""
    from botocore.exceptions import EndpointConnectionError

    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)
    scraper._s3.upload_tile = AsyncMock(  # pylint: disable=protected-access
        side_effect=EndpointConnectionError(endpoint_url="http://down:9000")
    )

    await scraper._run_sync()  # pylint: disable=protected-access

    # Every tile should be in the failed queue (added during the sweep) —
    # we did NOT wipe it with clear_failed_for_provider.
    from services.basemap_config import iter_tiles

    expected = len(list(iter_tiles(5, bbox)))
    assert len(await store.list_failed(provider.provider_id, 5)) == expected


# --------------------------------------------------------------------------- #
# Bucket lifecycle self-healing
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_lifecycle_applied_once_on_success(store):
    """ensure_lifecycle_expiration is called on the first sweep and latches afterwards."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)
    mock = AsyncMock(return_value=True)
    scraper._s3.ensure_lifecycle_expiration = mock  # pylint: disable=protected-access

    await scraper._run_sync()  # pylint: disable=protected-access
    await scraper._run_sync()  # pylint: disable=protected-access
    await scraper._run_sync()  # pylint: disable=protected-access

    # First success latches; no subsequent retries.
    assert mock.await_count == 1
    assert scraper._lifecycle_applied is True  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_lifecycle_retries_until_it_sticks(store):
    """A False return keeps the flag False; next cycle retries."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox)
    # Fail twice, then succeed on the third try.
    mock = AsyncMock(side_effect=[False, False, True, True])
    scraper._s3.ensure_lifecycle_expiration = mock  # pylint: disable=protected-access

    await scraper._run_sync()  # pylint: disable=protected-access
    assert scraper._lifecycle_applied is False  # pylint: disable=protected-access
    await scraper._run_sync()  # pylint: disable=protected-access
    assert scraper._lifecycle_applied is False  # pylint: disable=protected-access
    await scraper._run_sync()  # pylint: disable=protected-access
    assert scraper._lifecycle_applied is True  # pylint: disable=protected-access
    # After latching, no further retries even if we trigger more cycles.
    await scraper._run_sync()  # pylint: disable=protected-access
    assert mock.await_count == 3


# --------------------------------------------------------------------------- #
# Bounded (chunked) fan-out — basemap_scrape_fanout_window
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_chunked_sweep_processes_every_tile_exactly_once(store):
    """A tiny fan-out window must still cover every tile once, no dupes/drops."""
    provider = _make_provider(min_zoom=5, max_zoom=6)
    bbox = _make_bbox()
    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox, basemap_scrape_fanout_window=2)

    await scraper._run_sync()  # pylint: disable=protected-access

    expected = count_tiles(5, bbox) + count_tiles(6, bbox)
    assert len(http.calls) == expected
    assert len(set(http.calls)) == expected  # no tile fetched twice
    assert await store.get_cursor(provider.provider_id) is None  # clean finish


@pytest.mark.asyncio
async def test_chunked_sweep_resume_across_chunk_boundary(store):
    """Resume index is honoured even when it lands inside a chunk grid."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()

    from services.basemap_config import iter_tiles

    coords = list(iter_tiles(5, bbox))
    assert len(coords) >= 3, "bbox too small for this test"
    await store.set_cursor(provider.provider_id, 5, 2)

    http = FakeHttp()
    scraper = _make_scraper(store, http, provider, bbox, basemap_scrape_fanout_window=2)
    await scraper._run_sync()  # pylint: disable=protected-access

    assert len(http.calls) == len(coords) - 2
    assert await store.get_cursor(provider.provider_id) is None


class _CountingHttp:
    """Records peak concurrent download_tile calls to prove the fan-out bound."""

    def __init__(self):
        self.calls: list[str] = []
        self.in_flight = 0
        self.peak = 0

    async def download_tile(self, url: str):
        self.calls.append(url)
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        await asyncio.sleep(0)  # let all tasks in the chunk overlap
        self.in_flight -= 1
        return b"fake-bytes"


@pytest.mark.asyncio
async def test_chunked_sweep_bounds_inflight_tasks(store):
    """Concurrent in-flight fetches never exceed the fan-out window."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()

    from services.basemap_config import iter_tiles

    total = len(list(iter_tiles(5, bbox)))
    window = 3
    assert total > window, "need more tiles than the window to test the bound"

    http = _CountingHttp()
    scraper = _make_scraper(
        store, http, provider, bbox, basemap_scrape_fanout_window=window
    )
    await scraper._run_sync()  # pylint: disable=protected-access

    assert http.peak <= window, f"peak in-flight {http.peak} exceeded window {window}"
    assert len(http.calls) == total


@pytest.mark.asyncio
async def test_scrape_records_per_provider_dashboard_cycle(store):
    """Each scraped provider records a 'basemap' metrics cycle, so the dashboard
    shows progress during a sweep instead of only on full-sweep completion."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    http = FakeHttp()  # all tiles OK -> the provider downloads > 0
    scraper = _make_scraper(store, http, provider, bbox)

    metrics = MagicMock()
    metrics.record_sync_cycle = AsyncMock()
    scraper._metrics_store = metrics  # pylint: disable=protected-access

    await scraper._run_sync()  # pylint: disable=protected-access

    # One cycle for the single provider that actually scraped.
    assert metrics.record_sync_cycle.await_count == 1
    args = metrics.record_sync_cycle.await_args.args
    assert args[0] == "basemap"  # domain
    assert args[4] == len(http.calls) > 0  # downloaded count


@pytest.mark.asyncio
async def test_never_scraped_provider_is_due_now(store):
    """A provider with no cursor and no last_completed is due immediately:
    _compute_next_sleep must return 0, not defer a full scrape interval."""
    provider = _make_provider(min_zoom=5, max_zoom=5)
    bbox = _make_bbox()
    scraper = _make_scraper(store, FakeHttp(), provider, bbox)

    assert await store.get_cursor(provider.provider_id) is None
    assert await store.get_last_completed(provider.provider_id) is None

    sleep = await scraper._compute_next_sleep(999.0)  # pylint: disable=protected-access
    assert sleep == 0.0
