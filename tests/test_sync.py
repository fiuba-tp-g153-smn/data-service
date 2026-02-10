import pytest
from unittest.mock import AsyncMock, MagicMock
from clients.s3_client import S3Client


@pytest.mark.asyncio
async def test_sync_prefix_to_redis(mock_redis_client):
    """Verify that sync_prefix_to_redis downloads tiles and stores them in Redis."""
    client = S3Client("endpoint", "access", "secret", "bucket")

    # Mock S3 listing
    client._list_objects = AsyncMock(
        return_value=[
            {"Key": "band_13/tiles/tileset1_tiles/5/10/15.webp", "Size": 100},
            {"Key": "band_13/tiles/tileset1_tiles/5/10/16.webp", "Size": 200},
            {"Key": "band_13/tiles/tileset1_tiles/metadata.json", "Size": 50},
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
        "band_13/tiles/tileset1_tiles/",
        "band_13",
        "tileset1",
    )

    # Should have downloaded 2 .webp files (not the .json)
    assert downloaded == 2
    assert mock_redis_client.store_satellite_tile.call_count == 2


@pytest.mark.asyncio
async def test_sync_prefix_to_redis_no_objects(mock_redis_client):
    """Verify that sync returns 0 when no objects found."""
    client = S3Client("endpoint", "access", "secret", "bucket")

    client._list_objects = AsyncMock(return_value=[])

    mock_s3_client = AsyncMock()
    client._session.client = MagicMock()
    client._session.client.return_value.__aenter__ = AsyncMock(
        return_value=mock_s3_client
    )
    client._session.client.return_value.__aexit__ = AsyncMock(return_value=False)

    downloaded = await client.sync_prefix_to_redis(
        mock_redis_client,
        "band_13/tiles/tileset1_tiles/",
        "band_13",
        "tileset1",
    )

    assert downloaded == 0
    mock_redis_client.store_satellite_tile.assert_not_called()
