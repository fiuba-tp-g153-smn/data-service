"""
MinIO S3 Sync Client.

Provides sync functionality to download tiles from MinIO S3 bucket to local storage.
Used by data-service to periodically sync tiles for serving via REST API.
"""

import aioboto3
import asyncio
import logging
from pathlib import Path
from typing import List, Set

logger = logging.getLogger(__name__)


class MinioSyncClient:
    """
    Async MinIO sync client for tile downloads.

    Syncs tile directories from MinIO S3 bucket to local storage with
    incremental updates (only downloads new/changed files).

    Attributes:
        _endpoint: MinIO endpoint URL (e.g., "minio:9000")
        _access_key: MinIO access key
        _secret_key: MinIO secret key
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

    async def sync_prefix(
        self,
        s3_prefix: str,
        local_dir: Path,
        delete_orphans: bool = True,
    ) -> int:
        """
        Sync files from S3 prefix to local directory.

        Args:
            s3_prefix: S3 key prefix to sync (e.g., "band_13/tiles")
            local_dir: Local directory to sync to
            delete_orphans: Whether to delete local files not in S3

        Returns:
            Number of files downloaded
        """
        local_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Starting sync from s3://{self._bucket}/{s3_prefix} to {local_dir}")

        async with self._session.client(
            "s3",
            endpoint_url=self._get_endpoint_url(),
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        ) as s3_client:
            # List all objects in S3
            s3_objects = await self._list_objects(s3_client, s3_prefix)

            if not s3_objects:
                logger.info(f"No objects found under {s3_prefix}")
                return 0

            # Get existing local files
            local_files = self._get_local_files(local_dir, s3_prefix)

            # Determine files to download (new or updated)
            files_to_download = []
            s3_keys: Set[str] = set()

            for obj in s3_objects:
                s3_key = obj["Key"]
                s3_keys.add(s3_key)

                # Calculate local path
                relative_path = s3_key[len(s3_prefix):].lstrip("/")
                local_path = local_dir / relative_path

                # Check if we need to download
                if local_path not in local_files:
                    files_to_download.append((s3_key, local_path))
                elif obj.get("Size", 0) != local_files[local_path]:
                    # Size mismatch - re-download
                    files_to_download.append((s3_key, local_path))

            # Download files
            downloaded = 0
            if files_to_download:
                logger.info(f"Downloading {len(files_to_download)} files...")
                tasks = [
                    self._download_file_with_limit(s3_client, s3_key, local_path)
                    for s3_key, local_path in files_to_download
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                downloaded = sum(1 for r in results if r is True)

            # Delete orphan files if requested
            if delete_orphans:
                orphans_deleted = await self._delete_orphans(
                    local_dir, s3_prefix, s3_keys
                )
                if orphans_deleted > 0:
                    logger.info(f"Deleted {orphans_deleted} orphan files")

            logger.info(
                f"Sync completed: {downloaded} files downloaded, "
                f"{len(s3_objects)} total objects in S3"
            )
            return downloaded

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

    def _get_local_files(self, local_dir: Path, s3_prefix: str) -> dict:
        """Get map of local file paths to their sizes."""
        files = {}
        if local_dir.exists():
            for file_path in local_dir.rglob("*"):
                if file_path.is_file():
                    files[file_path] = file_path.stat().st_size
        return files

    async def _download_file_with_limit(
        self, s3_client, s3_key: str, local_path: Path
    ) -> bool:
        """Download a file with semaphore-controlled concurrency."""
        async with self._semaphore:
            return await self._download_file(s3_client, s3_key, local_path)

    async def _download_file(
        self, s3_client, s3_key: str, local_path: Path
    ) -> bool:
        """Download a single file from S3."""
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)

            response = await s3_client.get_object(Bucket=self._bucket, Key=s3_key)
            async with response["Body"] as stream:
                content = await stream.read()

            await asyncio.to_thread(local_path.write_bytes, content)
            logger.debug(f"Downloaded: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to download {s3_key}: {e}")
            return False

    async def _delete_orphans(
        self, local_dir: Path, s3_prefix: str, s3_keys: Set[str]
    ) -> int:
        """Delete local files that don't exist in S3."""
        deleted = 0
        if not local_dir.exists():
            return deleted

        for file_path in local_dir.rglob("*"):
            if file_path.is_file():
                relative_path = file_path.relative_to(local_dir)
                s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")

                if s3_key not in s3_keys:
                    try:
                        file_path.unlink()
                        deleted += 1
                        logger.debug(f"Deleted orphan: {file_path}")
                    except Exception as e:
                        logger.warning(f"Failed to delete orphan {file_path}: {e}")

        # Clean up empty directories
        for dir_path in sorted(local_dir.rglob("*"), reverse=True):
            if dir_path.is_dir() and not any(dir_path.iterdir()):
                try:
                    dir_path.rmdir()
                except Exception:
                    pass

        return deleted

    async def check_connection(self) -> bool:
        """Check if we can connect to MinIO."""
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
            logger.warning(f"MinIO connection check failed: {e}")
            return False
