"""
S3 Client.

Provides sync functionality to download tiles from S3 bucket and store
them directly in Redis. Used by SyncService for periodic satellite tile sync.
"""

import asyncio
import logging
from contextlib import AsyncExitStack
from typing import Any, AsyncIterator, List, Optional, cast

import aioboto3
from botocore.exceptions import ClientError
from types_aiobotocore_s3.client import S3Client as S3ClientType
from types_aiobotocore_s3.type_defs import ObjectIdentifierTypeDef

from clients.redis_client import RedisClient

logger = logging.getLogger(__name__)


class S3Client:  # pylint: disable=too-many-positional-arguments
    """
    Async S3 client for tile downloads.

    Maintains a persistent connection to S3 to avoid per-request
    connection overhead. Call connect() before use and close() on shutdown.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        max_concurrent_downloads: int,
        secure: bool = False,
    ):
        # pylint: disable=too-many-arguments
        self._endpoint = endpoint
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket
        self._secure = secure
        self._max_concurrent_downloads = max_concurrent_downloads
        self._semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self._session = aioboto3.Session()
        self._exit_stack: Optional[AsyncExitStack] = None
        self._client: Optional[S3ClientType] = None

    def _get_endpoint_url(self) -> str:
        protocol = "https" if self._secure else "http"
        return f"{protocol}://{self._endpoint}"

    async def connect(self) -> None:
        """Create persistent S3 client connection."""
        if self._client:
            return
        self._exit_stack = AsyncExitStack()
        ctx = self._session.client(
            "s3",
            endpoint_url=self._get_endpoint_url(),
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

        self._client = await self._exit_stack.enter_async_context(ctx)
        logger.info("S3 client connected to %s", self._get_endpoint_url())

    async def close(self) -> None:
        """Close the S3 client connection."""
        if self._exit_stack:
            await self._exit_stack.aclose()
            self._client = None
            self._exit_stack = None
            logger.info("S3 client closed")

    async def _ensure_connected(self) -> Any:
        """Auto-connect if not already connected. Returns the active client."""
        if not self._client:
            await self.connect()

        if self._client is None:
            raise RuntimeError("S3 client failed to connect")

        return self._client

    async def sync_prefix_to_redis(
        self,
        redis_client: RedisClient,
        s3_prefix: str,
        channel_dir: str,
        tileset_id: str,
        tile_ttl: Optional[int] = None,
    ) -> int:
        # pylint: disable=too-many-arguments
        """
        Download all tiles for a tileset from S3 and store directly in Redis.

        Args:
            redis_client: Redis client for storing tiles
            s3_prefix: S3 key prefix for this tileset (e.g., "tiles/band_13/20260740300213/")
            channel_dir: Channel directory name (e.g., "band_13")
            tileset_id: Tileset identifier
            tile_ttl: Optional TTL in seconds for stored tiles

        Returns:
            Number of tiles stored
        """
        await self._ensure_connected()
        s3_objects = await self._list_objects(s3_prefix)

        if not s3_objects:
            return 0

        # Filter only .webp tile files
        tile_objects = [obj for obj in s3_objects if obj["Key"].endswith(".webp")]

        if not tile_objects:
            return 0

        logger.info(
            "Downloading %d tiles for %s/%s",
            len(tile_objects),
            channel_dir,
            tileset_id,
        )

        tasks = [
            self._download_tile_to_redis(
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
        redis_client: RedisClient,
        s3_key: str,
        channel_dir: str,
        tileset_id: str,
        tile_ttl: Optional[int] = None,
    ) -> bool:
        # pylint: disable=too-many-arguments
        """Download a single tile from S3 and store in Redis."""
        if self._client is None:
            raise RuntimeError("S3 client is not connected")

        client = self._client
        async with self._semaphore:
            try:
                # Parse z/x/y from key: .../{tileset_id}/{z}/{x}/{y}.webp
                parts = s3_key.split("/")
                y_file = parts[-1]  # "{y}.webp"
                x = parts[-2]
                z = parts[-3]
                y = y_file.replace(".webp", "")

                response = await client.get_object(Bucket=self._bucket, Key=s3_key)
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
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to download %s to Redis: %s", s3_key, e)
                return False

    async def _list_objects(self, prefix: str) -> List[dict]:
        """List all objects under a prefix."""
        if self._client is None:
            raise RuntimeError("S3 client is not connected")

        client = self._client
        objects = []
        try:
            paginator = client.get_paginator("list_objects_v2")
            async for page in cast(
                AsyncIterator[dict],
                paginator.paginate(Bucket=self._bucket, Prefix=prefix),
            ):
                for obj in page.get("Contents", []):
                    if not obj["Key"].endswith("/"):
                        objects.append(obj)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error listing objects: %s", e)
        return objects

    ECMWF_TILES_PREFIX = "tiles/models/ecmwf/total_precipitation"
    ECMWF_COG_PREFIX = "cog/models/ecmwf/total_precipitation"

    @staticmethod
    def build_ecmwf_tile_key(
        forecast_ts: str, period_ts: str, z: int, x: int, y: int
    ) -> str:
        """Build S3 key for an ECMWF precipitation tile."""
        return f"tiles/models/ecmwf/total_precipitation/{forecast_ts}/{period_ts}/{z}/{x}/{y}.webp"

    @staticmethod
    def build_ecmwf_cog_key(forecast_ts: str, period_ts: str) -> str:
        """Build S3 key for an ECMWF precipitation COG."""
        return f"cog/models/ecmwf/total_precipitation/{forecast_ts}/{period_ts}.tif"

    async def sync_ecmwf_period_to_redis(
        self,
        redis_client: RedisClient,
        forecast_ts: str,
        period_ts: str,
        tile_ttl: int,
    ) -> int:
        """Download all tiles for an ECMWF period from S3 and store in Redis."""
        s3_prefix = f"{self.ECMWF_TILES_PREFIX}/{forecast_ts}/{period_ts}/"
        await self._ensure_connected()
        s3_objects = await self._list_objects(s3_prefix)

        tile_objects = [obj for obj in s3_objects if obj["Key"].endswith(".webp")]
        if not tile_objects:
            return 0

        logger.info(
            "Downloading %d ECMWF tiles for %s/%s",
            len(tile_objects),
            forecast_ts,
            period_ts,
        )

        tasks = [
            self._download_ecmwf_tile_to_redis(
                redis_client, obj["Key"], forecast_ts, period_ts, tile_ttl
            )
            for obj in tile_objects
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for r in results if r is True)

    async def _download_ecmwf_tile_to_redis(
        self,
        redis_client: RedisClient,
        s3_key: str,
        forecast_ts: str,
        period_ts: str,
        tile_ttl: int,
    ) -> bool:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Download a single ECMWF tile from S3 and store in Redis."""
        if self._client is None:
            raise RuntimeError("S3 client is not connected")

        client = self._client
        async with self._semaphore:
            try:
                parts = s3_key.split("/")
                y_file = parts[-1]
                x = parts[-2]
                z = parts[-3]
                y = y_file.replace(".webp", "")

                response = await client.get_object(Bucket=self._bucket, Key=s3_key)
                async with response["Body"] as stream:
                    content = await stream.read()

                await redis_client.store_ecmwf_tile(
                    forecast_ts, period_ts, int(z), int(x), int(y), content, ttl=tile_ttl
                )
                return True
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to download ECMWF tile %s: %s", s3_key, e)
                return False

    @staticmethod
    def build_satellite_tile_key(
        channel_dir: str, tileset_id: str, z: int, x: int, y: int
    ) -> str:
        """Build S3 key for a satellite tile."""
        return f"tiles/{channel_dir}/{tileset_id}/{z}/{x}/{y}.webp"

    @staticmethod
    def build_radar_tile_key(
        radar_id: str,
        variable_id: str,
        tileset_id: str,
        elevation_id: str,
        z: int,
        x: int,
        y: int,
    ) -> str:
        """Build S3 key for a radar tile."""
        return (
            f"tiles/radar/{radar_id}/{variable_id}/"
            f"{elevation_id}/{tileset_id}/{z}/{x}/{y}.webp"
        )

    async def sync_radar_prefix_to_redis(
        self,
        redis_client: RedisClient,
        s3_prefix: str,
        radar_id: str,
        variable_id: str,
        tileset_id: str,
        elevation_id: str,
        tile_ttl: int,
    ) -> int:
        # pylint: disable=too-many-arguments
        """Download all radar tiles for a tileset from S3 and store in Redis."""
        await self._ensure_connected()
        s3_objects = await self._list_objects(s3_prefix)

        if not s3_objects:
            return 0

        tile_objects = [obj for obj in s3_objects if obj["Key"].endswith(".webp")]
        if not tile_objects:
            return 0

        logger.info(
            "Downloading %d radar tiles for %s/%s/%s/%s",
            len(tile_objects),
            radar_id,
            variable_id,
            elevation_id,
            tileset_id,
        )

        tasks = [
            self._download_radar_tile_to_redis(
                redis_client,
                obj["Key"],
                radar_id,
                variable_id,
                tileset_id,
                elevation_id,
                tile_ttl,
            )
            for obj in tile_objects
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for r in results if r is True)

    async def _download_radar_tile_to_redis(
        self,
        redis_client: RedisClient,
        s3_key: str,
        radar_id: str,
        variable_id: str,
        tileset_id: str,
        elevation_id: str,
        tile_ttl: int,
    ) -> bool:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Download a single radar tile from S3 and store in Redis."""
        if self._client is None:
            raise RuntimeError("S3 client is not connected")

        client = self._client
        async with self._semaphore:
            try:
                parts = s3_key.split("/")
                y_file = parts[-1]
                x = parts[-2]
                z = parts[-3]
                y = y_file.replace(".webp", "")

                response = await client.get_object(Bucket=self._bucket, Key=s3_key)
                async with response["Body"] as stream:
                    content = await stream.read()

                await redis_client.store_radar_tile(
                    radar_id,
                    variable_id,
                    tileset_id,
                    elevation_id,
                    int(z),
                    int(x),
                    int(y),
                    content,
                    ttl=tile_ttl,
                )
                return True
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to download radar tile %s: %s", s3_key, e)
                return False

    async def download_tile(self, s3_key: str) -> Optional[bytes]:
        """Download a single tile from S3. Returns raw bytes or None."""
        client = await self._ensure_connected()
        try:
            response = await client.get_object(Bucket=self._bucket, Key=s3_key)
            async with response["Body"] as stream:
                return await stream.read()
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("Failed to download tile %s: %s", s3_key, e)
            return None

    async def object_exists(self, key: str) -> bool:
        """Check if an object exists in S3 using a HEAD request."""
        client = await self._ensure_connected()
        try:
            await client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            logger.error("Unexpected S3 error while checking object %s: %s", key, exc)
            raise

    async def check_connection(self) -> bool:
        """Check if we can connect to S3."""
        try:
            client = await self._ensure_connected()
            await client.head_bucket(Bucket=self._bucket)
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("S3 connection check failed: %s", e)
            return False

    async def get_subdirectories(self, prefix: str) -> List[str]:
        """
        List immediate subdirectories (prefixes) under a given prefix.
        Uses '/' as a delimiter.
        """
        client = await self._ensure_connected()
        subdirs = []
        if not prefix.endswith("/"):
            prefix += "/"

        try:
            paginator = client.get_paginator("list_objects_v2")
            async for page in cast(
                AsyncIterator[dict],
                paginator.paginate(Bucket=self._bucket, Prefix=prefix, Delimiter="/"),
            ):
                for common_prefix in page.get("CommonPrefixes", []):
                    subdirs.append(common_prefix["Prefix"])
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error listing subdirectories for %s: %s", prefix, e)

        return subdirs

    async def delete_prefix(self, prefix: str) -> bool:
        """Recursively delete all objects under a prefix."""
        client = await self._ensure_connected()
        try:
            objects_to_delete: List[ObjectIdentifierTypeDef] = []
            paginator = client.get_paginator("list_objects_v2")
            async for page in cast(
                AsyncIterator[dict],
                paginator.paginate(Bucket=self._bucket, Prefix=prefix),
            ):
                for obj in page.get("Contents", []):
                    objects_to_delete.append(ObjectIdentifierTypeDef(Key=obj["Key"]))

                    if len(objects_to_delete) >= 1000:
                        await self._delete_objects_batch(objects_to_delete)
                        objects_to_delete = []

            if objects_to_delete:
                await self._delete_objects_batch(objects_to_delete)

            logger.info("Deleted prefix: %s", prefix)
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error deleting prefix %s: %s", prefix, e)
            return False

    async def _delete_objects_batch(
        self, objects: List[ObjectIdentifierTypeDef]
    ) -> None:
        """Helper to delete a batch of objects."""
        if not objects:
            return

        if self._client is None:
            raise RuntimeError("S3 client is not connected")

        await self._client.delete_objects(
            Bucket=self._bucket, Delete={"Objects": objects, "Quiet": True}
        )
