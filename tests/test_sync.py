import pytest
from unittest.mock import AsyncMock, MagicMock
from clients.s3_client import S3Client
from services.radar_sync_strategy import RadarOnDemandStrategy


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
