import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from pathlib import Path
from clients.s3_client import S3Client

@pytest.mark.asyncio
async def test_sync_prefix_filtering():
    """Verify that sync_prefix filters files by extension."""
    client = S3Client("endpoint", "access", "secret", "bucket")
    
    # Mock internal methods
    client._list_objects = AsyncMock(return_value=[
        {"Key": "path/tile.webp", "Size": 100},
        {"Key": "path/metadata.json", "Size": 200},
        {"Key": "path/other.txt", "Size": 50},
    ])
    client._get_local_files = MagicMock(return_value={})
    client._download_file_with_limit = AsyncMock(return_value=True)
    client._delete_orphans = MagicMock(return_value=0)
    
    # Mock session and s3 client context manager
    mock_s3 = AsyncMock()
    client._session.client = MagicMock()
    client._session.client.return_value.__aenter__.return_value = mock_s3
    
    # Run sync with filtering
    downloaded = await client.sync_prefix(
        "path", 
        Path("/tmp"), 
        extensions=[".webp"]
    )
    
    # Verify results
    assert downloaded == 1
    
    # Check that only .webp file was downloaded
    client._download_file_with_limit.assert_called_once()
    args = client._download_file_with_limit.call_args[0]
    # args: (s3_client, s3_key, local_path)
    assert args[1] == "path/tile.webp"

@pytest.mark.asyncio
async def test_sync_prefix_no_filtering():
    """Verify that sync_prefix downloads all files if no extensions provided."""
    client = S3Client("endpoint", "access", "secret", "bucket")
    
    # Mock internal methods
    client._list_objects = AsyncMock(return_value=[
        {"Key": "path/tile.webp", "Size": 100},
        {"Key": "path/metadata.json", "Size": 200},
    ])
    client._get_local_files = MagicMock(return_value={})
    client._download_file_with_limit = AsyncMock(return_value=True)
    client._delete_orphans = MagicMock(return_value=0)
    
    # Mock session
    mock_s3 = AsyncMock()
    client._session.client = MagicMock()
    client._session.client.return_value.__aenter__.return_value = mock_s3
    
    # Run sync without filtering
    downloaded = await client.sync_prefix("path", Path("/tmp"))
    
    # Verify results
    assert downloaded == 2
    assert client._download_file_with_limit.call_count == 2
