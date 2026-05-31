import time

import pytest
from unittest.mock import AsyncMock, MagicMock
from clients.s3_client import S3Client
from services.radar_sync_strategy import RadarOnDemandStrategy
from services.sync_service import SyncService
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


def _make_sync_service(mock_s3, mock_redis, ecmwf_forecasts_to_keep=2):
    """Build a SyncService wired to mock S3/Redis clients without touching env."""
    settings = Settings.__new__(Settings)
    settings.sync_interval_seconds = 60
    settings.tile_ttl = 3600
    settings.ecmwf_tile_ttl = 86400
    settings.ecmwf_forecasts_to_keep = ecmwf_forecasts_to_keep

    service = SyncService.__new__(SyncService)
    service._settings = settings  # pylint: disable=protected-access
    service._sync_interval = 60  # pylint: disable=protected-access
    service._service_name = "Sync service"  # pylint: disable=protected-access
    service._sync_prefixes = []  # pylint: disable=protected-access
    service._client = mock_s3  # pylint: disable=protected-access
    service._redis_client = mock_redis  # pylint: disable=protected-access
    service._consecutive_failures = 0  # pylint: disable=protected-access
    service._total_cycles = 0  # pylint: disable=protected-access
    return service


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

    service = _make_sync_service(mock_s3, mock_redis_client)
    service._sync_prefixes = ["tiles/band_13"]  # pylint: disable=protected-access

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

    service = _make_sync_service(mock_s3, mock_redis_client)
    service._sync_prefixes = ["tiles/band_13"]  # pylint: disable=protected-access

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
async def test_sync_ecmwf_tp_downloads_new_periods_and_writes_index(mock_redis_client):
    """SyncService._sync_ecmwf_tp lists forecasts/periods and only downloads new ones."""
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

    service = _make_sync_service(mock_s3, mock_redis_client)

    downloaded, errors = (
        await service._sync_ecmwf_tp()
    )  # pylint: disable=protected-access

    assert errors == 0
    assert downloaded == 4 * 2  # one new period per forecast → two downloads
    assert mock_s3.sync_ecmwf_tp_period_to_redis.await_count == 2
    # Index updates always run, even when nothing was downloaded.
    assert mock_redis_client.store_ecmwf_tp_index.await_count == 2


@pytest.mark.asyncio
async def test_sync_ecmwf_tp_isolates_errors(mock_redis_client):
    """A failure inside _sync_ecmwf_tp is reported, not raised."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(side_effect=RuntimeError("boom"))

    service = _make_sync_service(mock_s3, mock_redis_client)

    downloaded, errors = (
        await service._sync_ecmwf_tp()
    )  # pylint: disable=protected-access

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

    service = _make_sync_service(mock_s3, mock_redis_client, ecmwf_forecasts_to_keep=1)

    await service._sync_ecmwf_tp()  # pylint: disable=protected-access

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

    service = _make_sync_service(mock_s3, mock_redis_client)

    downloaded, errors = (
        await service._sync_ecmwf_tp()
    )  # pylint: disable=protected-access

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
