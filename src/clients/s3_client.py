"""
S3 Client.

Provides sync functionality to download tiles from S3 bucket and store
them directly in Redis. Used by SyncService for periodic satellite tile sync.
"""

import asyncio
import logging
from typing import List, Optional

import aioboto3

from clients.redis_client import RedisClient

logger = logging.getLogger(__name__)


class S3Client:
    """
    Async S3 client for tile downloads.

    Downloads tiles from S3 and stores them directly in Redis,
    bypassing local filesystem entirely.

    Attributes:
        _endpoint: S3 endpoint URL (e.g., "minio:9000")
        _access_key: S3 access key
        _secret_key: S3 secret key
        _bucket: Source bucket name
        _secure: Whether to use HTTPS
        _max_concurrent_downloads: Maximum parallel downloads
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
        max_concurrent_downloads: int = 20,
    ):
        self._endpoint = endpoint
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket
        self._secure = secure
        self._max_concurrent_downloads = max_concurrent_downloads
        self._semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self._session = aioboto3.Session()

    def _get_endpoint_url(self) -> str:
        protocol = "https" if self._secure else "http"
        return f"{protocol}://{self._endpoint}"

    async def sync_prefix_to_redis(
        self,
        redis_client: RedisClient,
        s3_prefix: str,
        channel_dir: str,
        tileset_id: str,
        tile_ttl: Optional[int] = None,
    ) -> int:
        """
        Download all tiles for a tileset from S3 and store directly in Redis.

        Args:
            redis_client: Redis client for storing tiles
            s3_prefix: S3 key prefix for this tileset (e.g., "band_13/tiles/OR_ABI-.../")
            channel_dir: Channel directory name (e.g., "band_13")
            tileset_id: Tileset identifier
            tile_ttl: Optional TTL in seconds for stored tiles

        Returns:
            Number of tiles stored
        """
        async with self._session.client(
            "s3",
            endpoint_url=self._get_endpoint_url(),
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        ) as s3_client:
            s3_objects = await self._list_objects(s3_client, s3_prefix)

            if not s3_objects:
                return 0

            # Filter only .webp tile files
            tile_objects = [obj for obj in s3_objects if obj["Key"].endswith(".webp")]

            if not tile_objects:
                return 0

            logger.info(
                f"Downloading {len(tile_objects)} tiles for "
                f"{channel_dir}/{tileset_id}"
            )

            tasks = [
                self._download_tile_to_redis(
                    s3_client,
                    redis_client,
                    obj["Key"],
                    channel_dir,
                    tileset_id,
                    tile_ttl,
                )
                for obj in tile_objects
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            stored = sum(1 for r in results if r is True)

            return stored

    async def _download_tile_to_redis(
        self,
        s3_client,
        redis_client: RedisClient,
        s3_key: str,
        channel_dir: str,
        tileset_id: str,
        tile_ttl: Optional[int] = None,
    ) -> bool:
        """Download a single tile from S3 and store in Redis."""
        async with self._semaphore:
            try:
                # Parse z/x/y from key: .../tiles/{tileset_id}_tiles/{z}/{x}/{y}.webp
                parts = s3_key.split("/")
                # Find the tiles dir and extract z/x/y
                y_file = parts[-1]  # "{y}.webp"
                x = parts[-2]
                z = parts[-3]
                y = y_file.replace(".webp", "")

                response = await s3_client.get_object(Bucket=self._bucket, Key=s3_key)
                async with response["Body"] as stream:
                    content = await stream.read()

                await redis_client.store_satellite_tile(
                    channel_dir,
                    tileset_id,
                    int(z),
                    int(x),
                    int(y),
                    content,
                    ttl=tile_ttl,
                )
                return True
            except Exception as e:
                logger.error(f"Failed to download {s3_key} to Redis: {e}")
                return False

    async def _list_objects(self, s3_client, prefix: str) -> List[dict]:
        """List all objects under a prefix."""
        objects = []
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if not obj["Key"].endswith("/"):
                        objects.append(obj)
        except Exception as e:
            logger.error(f"Error listing objects: {e}")
        return objects

    @staticmethod
    def build_satellite_tile_key(
        channel_dir: str, tileset_id: str, z: int, x: int, y: int
    ) -> str:
        """Build S3 key for a satellite tile."""
        return f"{channel_dir}/tiles/{tileset_id}_tiles/{z}/{x}/{y}.webp"

    async def download_tile(self, s3_key: str) -> Optional[bytes]:
        """Download a single tile from S3. Returns raw bytes or None."""
        try:
            async with self._session.client(
                "s3",
                endpoint_url=self._get_endpoint_url(),
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
            ) as s3_client:
                response = await s3_client.get_object(Bucket=self._bucket, Key=s3_key)
                async with response["Body"] as stream:
                    return await stream.read()
        except Exception as e:
            logger.warning(f"Failed to download tile {s3_key}: {e}")
            return None

    async def check_connection(self) -> bool:
        """Check if we can connect to S3."""
        try:
            async with self._session.client(
                "s3",
                endpoint_url=self._get_endpoint_url(),
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
            ) as s3_client:
                await s3_client.head_bucket(Bucket=self._bucket)
                return True
        except Exception as e:
            logger.warning(f"S3 connection check failed: {e}")
            return False

    async def get_subdirectories(self, prefix: str) -> List[str]:
        """
        List immediate subdirectories (prefixes) under a given prefix.
        Uses '/' as a delimiter.
        """
        subdirs = []
        if not prefix.endswith("/"):
            prefix += "/"

        try:
            async with self._session.client(
                "s3",
                endpoint_url=self._get_endpoint_url(),
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
            ) as s3_client:
                paginator = s3_client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(
                    Bucket=self._bucket, Prefix=prefix, Delimiter="/"
                ):
                    for common_prefix in page.get("CommonPrefixes", []):
                        subdirs.append(common_prefix["Prefix"])
        except Exception as e:
            logger.error(f"Error listing subdirectories for {prefix}: {e}")

        return subdirs

    async def delete_prefix(self, prefix: str) -> bool:
        """
        Recursively delete all objects under a prefix.
        """
        try:
            async with self._session.client(
                "s3",
                endpoint_url=self._get_endpoint_url(),
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
            ) as s3_client:
                objects_to_delete = []
                paginator = s3_client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(
                    Bucket=self._bucket, Prefix=prefix
                ):
                    for obj in page.get("Contents", []):
                        objects_to_delete.append({"Key": obj["Key"]})

                        if len(objects_to_delete) >= 1000:
                            await self._delete_objects_batch(
                                s3_client, objects_to_delete
                            )
                            objects_to_delete = []

                if objects_to_delete:
                    await self._delete_objects_batch(s3_client, objects_to_delete)

            logger.info(f"Deleted prefix: {prefix}")
            return True
        except Exception as e:
            logger.error(f"Error deleting prefix {prefix}: {e}")
            return False

    async def _delete_objects_batch(self, s3_client, objects: List[dict]) -> None:
        """Helper to delete a batch of objects."""
        if not objects:
            return
        await s3_client.delete_objects(
            Bucket=self._bucket, Delete={"Objects": objects, "Quiet": True}
        )
