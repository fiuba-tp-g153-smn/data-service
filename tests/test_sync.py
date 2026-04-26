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
async def test_sync_ecmwf_downloads_new_periods_and_writes_index(mock_redis_client):
    """SyncService._sync_ecmwf lists forecasts/periods and only downloads new ones."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        side_effect=[
            # Top-level forecast listing
            [
                f"{S3Client.ECMWF_TILES_PREFIX}/20260330T1200Z/",
                f"{S3Client.ECMWF_TILES_PREFIX}/20260330T0000Z/",
            ],
            # Periods under forecast 20260330T1200Z
            [
                f"{S3Client.ECMWF_TILES_PREFIX}/20260330T1200Z/p1/",
                f"{S3Client.ECMWF_TILES_PREFIX}/20260330T1200Z/p2/",
            ],
            # Periods under forecast 20260330T0000Z
            [
                f"{S3Client.ECMWF_TILES_PREFIX}/20260330T0000Z/p3/",
            ],
        ]
    )
    mock_s3.sync_ecmwf_period_to_redis = AsyncMock(return_value=4)

    # Pretend p1 is already cached on the first forecast; nothing on the second.
    mock_redis_client.get_ecmwf_periods = AsyncMock(side_effect=[["p1"], []])

    service = _make_sync_service(mock_s3, mock_redis_client)

    downloaded, errors = await service._sync_ecmwf()  # pylint: disable=protected-access

    assert errors == 0
    assert downloaded == 4 * 2  # one new period per forecast → two downloads
    assert mock_s3.sync_ecmwf_period_to_redis.await_count == 2
    # Index updates always run, even when nothing was downloaded.
    assert mock_redis_client.store_ecmwf_index.await_count == 2


@pytest.mark.asyncio
async def test_sync_ecmwf_isolates_errors(mock_redis_client):
    """A failure inside _sync_ecmwf is reported, not raised."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(side_effect=RuntimeError("boom"))

    service = _make_sync_service(mock_s3, mock_redis_client)

    downloaded, errors = await service._sync_ecmwf()  # pylint: disable=protected-access

    assert downloaded == 0
    assert errors == 1


@pytest.mark.asyncio
async def test_sync_ecmwf_respects_forecasts_to_keep(mock_redis_client):
    """Only the top N forecasts are processed (sorted descending)."""
    mock_s3 = AsyncMock()
    mock_s3.get_subdirectories = AsyncMock(
        side_effect=[
            [
                f"{S3Client.ECMWF_TILES_PREFIX}/20260330T1200Z/",
                f"{S3Client.ECMWF_TILES_PREFIX}/20260330T0000Z/",
                f"{S3Client.ECMWF_TILES_PREFIX}/20260329T1200Z/",
            ],
            [],  # periods for 20260330T1200Z
        ]
    )
    mock_s3.sync_ecmwf_period_to_redis = AsyncMock(return_value=0)

    service = _make_sync_service(mock_s3, mock_redis_client, ecmwf_forecasts_to_keep=1)

    await service._sync_ecmwf()  # pylint: disable=protected-access

    # Only the most recent forecast queried for periods (1 top-level + 1 nested call).
    assert mock_s3.get_subdirectories.await_count == 2
