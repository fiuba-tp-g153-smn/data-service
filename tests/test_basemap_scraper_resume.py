"""Unit tests for resumable-scrape wiring in `BasemapScraperService`."""

import time
from types import SimpleNamespace
from typing import Dict, Iterable, Set, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from clients.basemap_state_store import BasemapStateStore, Cursor
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
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class FakeHttp:
    """Stand-in for `HttpTileClient.download_tile` with scripted outcomes."""

    def __init__(self, fail_tiles: Iterable[Tuple[int, int, int]] = ()):
        self._fail_tiles: Set[Tuple[int, int, int]] = set(fail_tiles)
        self.calls: list[str] = []

    async def download_tile(self, url: str):
        self.calls.append(url)
        parts = url.rstrip(".png").split("/")
        z, x, y = int(parts[-3]), int(parts[-2]), int(parts[-1])
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
    return BasemapScraperService(
        settings=settings,  # type: ignore[arg-type]
        s3_client=s3,
        redis_client=redis,
        http_client=http,  # type: ignore[arg-type]
        state_store=store_,
        providers=providers,
        bbox=bbox,
        tile_ttl=60,
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
