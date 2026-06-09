import time

import pytest
from unittest.mock import AsyncMock, MagicMock
from clients.s3_client import S3Client
from services.ecmwf_mslp_sync_service import EcmwfMslpSyncService
from services.ecmwf_tp_sync_service import EcmwfTpSyncService
from services.radar_sync_service import RadarSyncService
from services.radar_sync_strategy import RadarOnDemandStrategy
from services.satellite_sync_service import SatelliteSyncService
from services.wrf_sync_service import WrfSyncService
from settings import Settings


@pytest.mark.asyncio
async def test_sync_prefix_to_redis(mock_redis_client):
    """Verify that sync_prefix_to_redis downloads tiles and stores them in Redis."""
    client = S3Client(
        "endpoint", "access", "secret", "bucket", max_concurrent_downloads=5
    )

    # Mock S3 listing
    client._list_objects = AsyncMock(
        return_value=[
            {"Key": "tiles/band_13/tileset1/5/10/15.webp", "Size": 100},
            {"Key": "tiles/band_13/tileset1/5/10/16.webp", "Size": 200},
            {"Key": "tiles/band_13/tileset1/metadata.json", "Size": 50},
        ]
    )

    # Mock S3 get_object
    mock_body = AsyncMock()
    mock_body.read = AsyncMock(return_value=b"fake-tile-data")
    mock_s3_client = AsyncMock()
    mock_s3_client.get_object = AsyncMock(return_value={"Body": mock_body})

    # Mock session context manager
    client._session.client = MagicMock()
    client._session.client.return_value.__aenter__ = AsyncMock(
        return_value=mock_s3_client
    )
    client._session.client.return_value.__aexit__ = AsyncMock(return_value=False)

    # Run sync
    downloaded = await client.sync_prefix_to_redis(
        mock_redis_client,
        "tiles/band_13/tileset1/",
        "band_13",
        "tileset1",
    )

    # Should have downloaded 2 .webp files (not the .json)
    assert downloaded == 2
    assert mock_redis_client.store_satellite_tile.call_count == 2


@pytest.mark.asyncio
async def test_sync_prefix_to_redis_no_objects(mock_redis_client):
    """Verify that sync returns 0 when no objects found."""
    client = S3Client(
        "endpoint", "access", "secret", "bucket", max_concurrent_downloads=5
    )

    client._list_objects = AsyncMock(return_value=[])

    mock_s3_client = AsyncMock()
    client._session.client = MagicMock()
    client._session.client.return_value.__aenter__ = AsyncMock(
        return_value=mock_s3_client
    )
    client._session.client.return_value.__aexit__ = AsyncMock(return_value=False)

    downloaded = await client.sync_prefix_to_redis(
        mock_redis_client,
        "tiles/band_13/tileset1/",
        "band_13",
        "tileset1",
    )

    assert downloaded == 0
    mock_redis_client.store_satellite_tile.assert_not_called()


def test_build_satellite_tile_key_uses_tiles_root():
    """Satellite tile keys must be rooted at tiles/<band_id>/..."""
    key = S3Client.build_satellite_tile_key("band_13", "20260740300213", 5, 10, 15)
    assert key == "tiles/band_13/20260740300213/5/10/15.webp"


def test_build_radar_tile_key_splits_elevation_and_timestamp():
    """Radar tile keys must use .../<elevation>/<tileset_id>/... hierarchy."""
    key = S3Client.build_radar_tile_key(
        "RMA1", "DBZH", "20260114T170328Z", "elev0", 5, 10, 15
    )
    assert key == "tiles/radar/RMA1/DBZH/elev0/20260114T170328Z/5/10/15.webp"


@pytest.mark.asyncio
async def test_radar_on_demand_lists_new_elevations_and_tilesets(mock_redis_client):
    """On-demand radar listing must read elevation and tileset as separate folders."""
    mock_redis_client.get_cached_listing = AsyncMock(return_value=None)
    mock_redis_client.cache_listing = AsyncMock()

    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        side_effect=[
            [
                "tiles/radar/RMA1/DBZH/elev0/",
                "tiles/radar/RMA1/DBZH/elev1/",
            ],
            [
                "tiles/radar/RMA1/DBZH/elev0/20260114T170328Z/",
                "tiles/radar/RMA1/DBZH/elev0/20260114T160328Z/",
            ],
        ]
    )

    strategy = RadarOnDemandStrategy(
        redis_client=mock_redis_client,
        s3_client=mock_s3,
        tile_ttl=3600,
        listing_ttl=30,
    )

    elevations = await strategy.list_elevations("RMA1", "DBZH")
    tilesets = await strategy.list_tilesets("RMA1", "DBZH", "elev0")

    assert elevations == ["elev0", "elev1"]
    assert tilesets == ["20260114T170328Z", "20260114T160328Z"]


# ── Per-product sync service builders ────────────────────────────────────────
# Each product now syncs on its own DomainSyncService subclass. The builders
# construct one via __new__() and wire mock S3/Redis clients without touching
# env (mirrors the per-strategy tests). Test files are excluded from pylint.


def _make_settings(ecmwf_forecasts_to_keep=2, wrf_inits_to_keep=2):
    settings = Settings.__new__(Settings)
    settings.sync_interval_seconds = 60
    settings.sync_min_sleep_seconds = 10
    settings.sync_domain_timeout_seconds = 300
    settings.wrf_sync_interval_seconds = 120
    settings.wrf_sync_timeout_seconds = 1200
    settings.tile_ttl = 3600
    settings.ecmwf_tile_ttl = 86400
    settings.ecmwf_forecasts_to_keep = ecmwf_forecasts_to_keep
    settings.ecmwf_mslp_geojson_ttl = 86400
    settings.wrf_tile_ttl = 86400
    settings.wrf_geojson_ttl = 86400
    settings.wrf_inits_to_keep = wrf_inits_to_keep
    return settings


def _wire(service, mock_s3, mock_redis, settings):
    """Populate a DomainSyncService subclass built via __new__()."""
    service._settings = settings
    service._sync_interval = settings.sync_interval_seconds
    service._min_sleep = settings.sync_min_sleep_seconds
    service._service_name = "test sync"
    service._domain = "test"
    service._lock_path = "/tmp/test_sync.lock"
    service._timeout = 300
    service._s3_concurrency = 5
    service._client = mock_s3
    service._redis_client = mock_redis
    service._metrics_store = None
    service._consecutive_failures = 0
    service._total_cycles = 0
    return service


def _make_satellite(mock_s3, mock_redis, prefixes=None):
    service = SatelliteSyncService.__new__(SatelliteSyncService)
    service._sync_prefixes = prefixes if prefixes is not None else []
    return _wire(service, mock_s3, mock_redis, _make_settings())


def _make_radar(mock_s3, mock_redis):
    return _wire(
        RadarSyncService.__new__(RadarSyncService),
        mock_s3,
        mock_redis,
        _make_settings(),
    )


def _make_ecmwf_tp(mock_s3, mock_redis, ecmwf_forecasts_to_keep=2):
    return _wire(
        EcmwfTpSyncService.__new__(EcmwfTpSyncService),
        mock_s3,
        mock_redis,
        _make_settings(ecmwf_forecasts_to_keep=ecmwf_forecasts_to_keep),
    )


def _make_ecmwf_mslp(mock_s3, mock_redis):
    return _wire(
        EcmwfMslpSyncService.__new__(EcmwfMslpSyncService),
        mock_s3,
        mock_redis,
        _make_settings(),
    )


def _make_wrf(mock_s3, mock_redis, wrf_inits_to_keep=2):
    return _wire(
        WrfSyncService.__new__(WrfSyncService),
        mock_s3,
        mock_redis,
        _make_settings(wrf_inits_to_keep=wrf_inits_to_keep),
    )


@pytest.mark.asyncio
async def test_sync_satellite_trims_expired_each_cycle(mock_redis_client):
    """Trim runs once per prefix even when no new tilesets are found."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        return_value=[
            "tiles/band_13/20260740300213/",
            "tiles/band_13/20260740400213/",
        ]
    )
    # Both S3 tilesets are already indexed -> zero new tilesets this cycle.
    mock_redis_client.get_satellite_tilesets = AsyncMock(
        return_value=["20260740300213", "20260740400213"]
    )

    service = _make_satellite(mock_s3, mock_redis_client, prefixes=["tiles/band_13"])

    downloaded, errors = await service._sync_satellite_prefixes()

    assert downloaded == 0
    assert errors == 0
    mock_s3.sync_prefix_to_redis.assert_not_called()
    mock_redis_client.add_satellite_tileset.assert_not_called()

    # Trim still fires once, bounding the index regardless of new arrivals.
    mock_redis_client.trim_satellite_index.assert_awaited_once()
    channel_dir, cutoff = mock_redis_client.trim_satellite_index.await_args.args
    assert channel_dir == "band_13"
    # cutoff = now - tile_ttl(3600); a recent epoch, well below "now".
    assert isinstance(cutoff, float)
    assert cutoff < time.time() - 3599


@pytest.mark.asyncio
async def test_sync_satellite_scores_new_tileset_with_insertion_time(mock_redis_client):
    """A newly-seen tileset is indexed with an insertion-time (epoch float) score."""
    before = time.time()
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        return_value=["tiles/band_13/20260740300213/"]
    )
    mock_s3.sync_prefix_to_redis = AsyncMock(return_value=4)
    mock_redis_client.get_satellite_tilesets = AsyncMock(return_value=[])

    service = _make_satellite(mock_s3, mock_redis_client, prefixes=["tiles/band_13"])

    downloaded, errors = await service._sync_satellite_prefixes()

    assert downloaded == 4
    assert errors == 0
    mock_redis_client.add_satellite_tileset.assert_awaited_once()
    args = mock_redis_client.add_satellite_tileset.await_args
    assert args.args[0] == "band_13"
    assert args.args[1] == "20260740300213"
    score = args.args[2]
    assert isinstance(score, float)
    assert before <= score <= time.time()
    assert args.kwargs["ttl"] == service._settings.tile_ttl
    mock_redis_client.trim_satellite_index.assert_awaited_once()


@pytest.mark.asyncio
async def test_sync_satellite_skips_indexing_on_zero_download(mock_redis_client):
    """A tileset whose download stores 0 tiles is NOT indexed (regression).

    Guards against poisoning idx:sat:{channel}: a transient empty/failed S3 read
    must not be cached as "present" and then skipped (in full mode = 404) until
    the next trim ~tile_ttl later. The download is retried next cycle instead.
    """
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        return_value=["tiles/glm_fed/20260611550000/"]
    )
    # Download stores nothing (e.g. transient S3 listing failure -> [] -> 0).
    mock_s3.sync_prefix_to_redis = AsyncMock(return_value=0)
    mock_redis_client.get_satellite_tilesets = AsyncMock(return_value=[])

    service = _make_satellite(mock_s3, mock_redis_client, prefixes=["tiles/glm_fed"])

    downloaded, errors = await service._sync_satellite_prefixes()

    assert downloaded == 0
    assert errors == 0
    # The download WAS attempted...
    mock_s3.sync_prefix_to_redis.assert_awaited_once()
    # ...but the tileset must NOT be indexed, so it stays "new" and is retried.
    mock_redis_client.add_satellite_tileset.assert_not_awaited()
    # Trim still runs once per prefix regardless.
    mock_redis_client.trim_satellite_index.assert_awaited_once()


@pytest.mark.asyncio
async def test_sync_radar_trims_expired_each_cycle(mock_redis_client):
    """Radar trim runs once per elevation even when no new tilesets are found."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        side_effect=[
            ["tiles/radar/RMA1/"],  # radars
            ["tiles/radar/RMA1/DBZH/"],  # variables
            ["tiles/radar/RMA1/DBZH/elev0/"],  # elevations
            ["tiles/radar/RMA1/DBZH/elev0/ts1/"],  # tilesets under elev0
        ]
    )
    # The S3 tileset is already indexed -> zero new tilesets this cycle.
    mock_redis_client.get_radar_tilesets = AsyncMock(return_value=["ts1"])

    service = _make_radar(mock_s3, mock_redis_client)

    downloaded, errors = await service._sync_radar()

    assert errors == 0
    assert downloaded == 0
    mock_s3.sync_radar_prefix_to_redis.assert_not_called()
    mock_redis_client.add_radar_index.assert_not_called()

    mock_redis_client.trim_radar_index.assert_awaited_once()
    radar, var, elev, cutoff = mock_redis_client.trim_radar_index.await_args.args
    assert (radar, var, elev) == ("RMA1", "DBZH", "elev0")
    assert isinstance(cutoff, float)
    assert cutoff < time.time() - 3599  # now - tile_ttl(3600)


@pytest.mark.asyncio
async def test_sync_radar_scores_new_tileset_with_insertion_time(mock_redis_client):
    """A newly-seen radar tileset is indexed with an insertion-time (epoch float) score."""
    before = time.time()
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        side_effect=[
            ["tiles/radar/RMA1/"],
            ["tiles/radar/RMA1/DBZH/"],
            ["tiles/radar/RMA1/DBZH/elev0/"],
            ["tiles/radar/RMA1/DBZH/elev0/ts1/"],
        ]
    )
    mock_s3.sync_radar_prefix_to_redis = AsyncMock(return_value=3)
    mock_redis_client.get_radar_tilesets = AsyncMock(return_value=[])

    service = _make_radar(mock_s3, mock_redis_client)

    downloaded, errors = await service._sync_radar()

    assert errors == 0
    assert downloaded == 3
    mock_redis_client.add_radar_index.assert_awaited_once()
    args = mock_redis_client.add_radar_index.await_args
    assert args.args[:4] == ("RMA1", "DBZH", "elev0", "ts1")
    score = args.args[4]
    assert isinstance(score, float)
    assert before <= score <= time.time()
    assert args.kwargs["ttl"] == service._settings.tile_ttl
    mock_redis_client.trim_radar_index.assert_awaited_once()


@pytest.mark.asyncio
async def test_sync_ecmwf_tp_downloads_new_periods_and_writes_index(mock_redis_client):
    """_sync_ecmwf_tp lists forecasts/periods and only downloads new ones."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        side_effect=[
            # Top-level forecast listing
            [
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T1200Z/",
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T0000Z/",
            ],
            # Periods under forecast 20260330T1200Z
            [
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T1200Z/20260330T1500Z/",
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T1200Z/20260330T1800Z/",
            ],
            # Periods under forecast 20260330T0000Z
            [
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T0000Z/20260330T0300Z/",
            ],
        ]
    )
    mock_s3.sync_ecmwf_tp_period_to_redis = AsyncMock(return_value=4)

    # Pretend the first centered period is already cached on forecast 1; nothing on forecast 2.
    mock_redis_client.get_ecmwf_tp_periods = AsyncMock(
        side_effect=[["20260330T1500Z"], []]
    )

    service = _make_ecmwf_tp(mock_s3, mock_redis_client)

    downloaded, errors = await service._sync_ecmwf_tp()

    assert errors == 0
    assert downloaded == 4 * 2  # one new period per forecast → two downloads
    assert mock_s3.sync_ecmwf_tp_period_to_redis.await_count == 2
    # Index updates always run, even when nothing was downloaded.
    assert mock_redis_client.store_ecmwf_tp_index.await_count == 2
    # Forecasts index is reconciled to the active set each cycle.
    mock_redis_client.prune_ecmwf_tp_forecasts.assert_awaited_once_with(
        ["20260330T1200Z", "20260330T0000Z"]
    )


@pytest.mark.asyncio
async def test_sync_ecmwf_tp_isolates_errors(mock_redis_client):
    """A failure inside _sync_ecmwf_tp is reported, not raised."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(side_effect=RuntimeError("boom"))

    service = _make_ecmwf_tp(mock_s3, mock_redis_client)

    downloaded, errors = await service._sync_ecmwf_tp()

    assert downloaded == 0
    assert errors == 1


@pytest.mark.asyncio
async def test_sync_ecmwf_tp_respects_forecasts_to_keep(mock_redis_client):
    """Only the top N forecasts are processed (sorted descending)."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        side_effect=[
            [
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T1200Z/",
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T0000Z/",
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260329T1200Z/",
            ],
            [],  # periods for 20260330T1200Z
        ]
    )
    mock_s3.sync_ecmwf_tp_period_to_redis = AsyncMock(return_value=0)

    service = _make_ecmwf_tp(mock_s3, mock_redis_client, ecmwf_forecasts_to_keep=1)

    await service._sync_ecmwf_tp()

    # Only the most recent forecast queried for periods (1 top-level + 1 nested call).
    assert mock_s3.get_subdirectories.await_count == 2


@pytest.mark.asyncio
async def test_sync_ecmwf_tp_filters_old_format_periods(mock_redis_client):
    """Periods that don't match the centered single-timestamp format are skipped."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        side_effect=[
            [f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T1200Z/"],
            [
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T1200Z/20260330T1500Z/",
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T1200Z/"
                "20260330T1200Z-20260330T1500Z/",
                f"{S3Client.ECMWF_TP_TILES_PREFIX}/20260330T1200Z/20260330T1800Z/",
            ],
        ]
    )
    mock_s3.sync_ecmwf_tp_period_to_redis = AsyncMock(return_value=1)
    mock_redis_client.get_ecmwf_tp_periods = AsyncMock(return_value=[])

    service = _make_ecmwf_tp(mock_s3, mock_redis_client)

    downloaded, errors = await service._sync_ecmwf_tp()

    assert errors == 0
    # Only the two centered periods are downloaded; the legacy one is skipped.
    assert mock_s3.sync_ecmwf_tp_period_to_redis.await_count == 2
    assert downloaded == 2

    # Verify the period_ts arguments passed to sync_ecmwf_tp_period_to_redis are the new ones.
    period_ts_args = [
        call.args[2] for call in mock_s3.sync_ecmwf_tp_period_to_redis.await_args_list
    ]
    assert period_ts_args == ["20260330T1500Z", "20260330T1800Z"]

    # Index also contains only the centered periods.
    mock_redis_client.store_ecmwf_tp_index.assert_awaited_once()
    indexed_periods = mock_redis_client.store_ecmwf_tp_index.await_args.args[1]
    assert indexed_periods == ["20260330T1500Z", "20260330T1800Z"]


@pytest.mark.asyncio
async def test_sync_ecmwf_mslp_prunes_forecasts(mock_redis_client):
    """_sync_ecmwf_mslp reconciles the forecasts index to the active set."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        return_value=[
            f"{S3Client.ECMWF_MSLP_COG_PREFIX}/20260330T1200Z/",
            f"{S3Client.ECMWF_MSLP_COG_PREFIX}/20260330T0000Z/",
        ]
    )
    mock_s3.list_object_basenames = AsyncMock(return_value=["20260330T1500Z"])
    mock_s3.sync_ecmwf_mslp_forecast_to_redis = AsyncMock(return_value=1)
    mock_redis_client.get_ecmwf_mslp_timestamps = AsyncMock(return_value=[])

    service = _make_ecmwf_mslp(mock_s3, mock_redis_client)

    downloaded, errors = await service._sync_ecmwf_mslp()

    assert errors == 0
    mock_redis_client.prune_ecmwf_mslp_forecasts.assert_awaited_once_with(
        ["20260330T1200Z", "20260330T0000Z"]
    )


@pytest.mark.asyncio
async def test_sync_wrf_respects_inits_to_keep(mock_redis_client):
    """Only the newest N init runs per product are walked, and the index is
    reconciled to that active set."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        side_effect=[
            [f"{S3Client.WRF_TILES_PREFIX}/precip/"],  # products
            [
                f"{S3Client.WRF_TILES_PREFIX}/precip/20260430_000000/",
                f"{S3Client.WRF_TILES_PREFIX}/precip/20260430_060000/",
                f"{S3Client.WRF_TILES_PREFIX}/precip/20260429_180000/",
            ],  # init runs (unsorted)
            [],  # steps for the single newest init
        ]
    )

    service = _make_wrf(mock_s3, mock_redis_client, wrf_inits_to_keep=1)

    downloaded, errors = await service._sync_wrf()

    assert (downloaded, errors) == (0, 0)
    # products + inits + steps(newest only) == 3 listing calls (not 5).
    assert mock_s3.get_subdirectories.await_count == 3
    mock_redis_client.prune_wrf_inits.assert_awaited_once_with(
        "precip", ["20260430_060000"]
    )


@pytest.mark.asyncio
async def test_sync_wrf_skips_prune_when_listing_empty(mock_redis_client):
    """A product whose init listing comes back empty (e.g. transient S3 error)
    must NOT prune, so the index isn't wiped."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        side_effect=[
            [f"{S3Client.WRF_TILES_PREFIX}/precip/"],  # products
            [],  # init runs empty
        ]
    )

    service = _make_wrf(mock_s3, mock_redis_client)

    downloaded, errors = await service._sync_wrf()

    assert (downloaded, errors) == (0, 0)
    mock_redis_client.prune_wrf_inits.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_wrf_overlays_skip_when_marker_present(mock_redis_client):
    """A step marked overlays-complete costs zero S3 calls."""
    mock_s3 = AsyncMock()
    mock_redis_client.is_wrf_overlays_complete = AsyncMock(return_value=True)

    service = _make_wrf(mock_s3, mock_redis_client)

    await service._sync_wrf_overlays("precip", "20260430_060000", "F012")

    mock_s3.list_wrf_layers.assert_not_awaited()
    mock_redis_client.set_wrf_overlays_complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_wrf_overlays_latches_marker_when_complete(mock_redis_client):
    """When Redis already mirrors every S3 layer, latch the marker and do no GETs."""
    mock_s3 = AsyncMock()
    mock_s3.list_wrf_layers = AsyncMock(return_value=["barbs", "isobars"])
    mock_redis_client.is_wrf_overlays_complete = AsyncMock(return_value=False)
    mock_redis_client.get_wrf_layers = AsyncMock(return_value=["barbs", "isobars"])

    service = _make_wrf(mock_s3, mock_redis_client)

    await service._sync_wrf_overlays("precip", "20260430_060000", "F012")

    mock_redis_client.set_wrf_overlays_complete.assert_awaited_once()
    mock_s3.sync_wrf_geojson_to_redis.assert_not_awaited()
    mock_redis_client.add_wrf_layers.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_wrf_overlays_downloads_missing_without_latching(mock_redis_client):
    """A step missing layers downloads them and is NOT marked complete (it is
    re-checked next cycle until S3 and Redis agree)."""
    mock_s3 = AsyncMock()
    mock_s3.list_wrf_layers = AsyncMock(return_value=["barbs", "isobars"])
    mock_s3.sync_wrf_geojson_to_redis = AsyncMock(return_value=True)
    mock_redis_client.is_wrf_overlays_complete = AsyncMock(return_value=False)
    mock_redis_client.get_wrf_layers = AsyncMock(return_value=["barbs"])

    service = _make_wrf(mock_s3, mock_redis_client)

    await service._sync_wrf_overlays("precip", "20260430_060000", "F012")

    # Only the missing layer is fetched.
    assert mock_s3.sync_wrf_geojson_to_redis.await_count == 1
    mock_redis_client.set_wrf_overlays_complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_wrf_error_isolation(mock_redis_client):
    """A listing failure surfaces as one error, not an exception."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(side_effect=RuntimeError("boom"))

    service = _make_wrf(mock_s3, mock_redis_client)

    downloaded, errors = await service._sync_wrf()

    assert (downloaded, errors) == (0, 1)
