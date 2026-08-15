"""
S3 Client.

Provides sync functionality to download tiles from S3 bucket and store
them directly in Redis. Used by SyncService for periodic satellite tile sync.
"""

# pylint: disable=too-many-lines

import asyncio
import logging
from contextlib import AsyncExitStack
from typing import Any, AsyncIterator, List, Optional, cast

import aioboto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from types_aiobotocore_s3.client import S3Client as S3ClientType
from types_aiobotocore_s3.type_defs import ObjectIdentifierTypeDef

from clients.redis_client import RedisClient

logger = logging.getLogger(__name__)


class S3Client:  # pylint: disable=too-many-positional-arguments,too-many-instance-attributes
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
        connect_timeout: Optional[float] = None,
        read_timeout: Optional[float] = None,
        max_attempts: Optional[int] = None,
    ):
        # pylint: disable=too-many-arguments
        self._endpoint = endpoint
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket
        self._secure = secure
        self._max_concurrent_downloads = max_concurrent_downloads
        # Timeouts/retries are applied per-operation via a botocore Config so a
        # stalled endpoint fails fast (and retries a bounded number of times)
        # instead of hanging a request — and the sync loop — indefinitely. Built
        # once here; None means "keep botocore defaults".
        self._config = self._build_config(connect_timeout, read_timeout, max_attempts)
        self._semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self._session = aioboto3.Session()
        self._exit_stack: Optional[AsyncExitStack] = None
        self._client: Optional[S3ClientType] = None

    @staticmethod
    def _build_config(
        connect_timeout: Optional[float],
        read_timeout: Optional[float],
        max_attempts: Optional[int],
    ) -> Optional[Config]:
        """Build a botocore Config from the configured timeouts/retries.

        Returns None when nothing is configured so botocore keeps its defaults.
        """
        if connect_timeout is None and read_timeout is None and max_attempts is None:
            return None
        kwargs: dict = {}
        if connect_timeout is not None:
            kwargs["connect_timeout"] = connect_timeout
        if read_timeout is not None:
            kwargs["read_timeout"] = read_timeout
        if max_attempts is not None:
            kwargs["retries"] = {"max_attempts": max_attempts, "mode": "standard"}
        return Config(**kwargs)

    def _get_endpoint_url(self) -> str:
        protocol = "https" if self._secure else "http"
        return f"{protocol}://{self._endpoint}"

    async def connect(self) -> None:
        """Create persistent S3 client connection."""
        if self._client:
            return
        self._exit_stack = AsyncExitStack()
        client_kwargs: dict = {
            "endpoint_url": self._get_endpoint_url(),
            "aws_access_key_id": self._access_key,
            "aws_secret_access_key": self._secret_key,
        }
        if self._config is not None:
            client_kwargs["config"] = self._config
        ctx = self._session.client("s3", **client_kwargs)

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

    async def list_object_keys(self, prefix: str) -> List[str]:
        """List full S3 keys under `prefix` (recursive, excludes directory markers).

        Use when you need every key under a prefix, regardless of depth — e.g.
        the weather-stations read service walking
        `weather-stations/snapshots/{Y}/{M}/{D}/{H}/...`. Returns `[]` on error
        instead of raising, mirroring the other read-path tolerance in this
        client.
        """
        objects = await self._list_objects(prefix)
        return [obj["Key"] for obj in objects]

    async def _list_objects(self, prefix: str, delimiter: str = "") -> List[dict]:
        """List all objects under a prefix.

        With ``delimiter="/"`` only objects directly under the prefix are
        returned — deeper sub-prefixes are collapsed server-side, so large
        subtrees are never fetched or parsed.
        """
        if self._client is None:
            raise RuntimeError("S3 client is not connected")

        client = self._client
        objects = []
        try:
            paginator = client.get_paginator("list_objects_v2")
            if delimiter:
                pages = paginator.paginate(
                    Bucket=self._bucket, Prefix=prefix, Delimiter=delimiter
                )
            else:
                pages = paginator.paginate(Bucket=self._bucket, Prefix=prefix)
            async for page in cast(AsyncIterator[dict], pages):
                for obj in page.get("Contents", []):
                    if not obj["Key"].endswith("/"):
                        objects.append(obj)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error listing objects: %s", e)
        return objects

    WRF_TILES_PREFIX = "tiles/wrf"
    WRF_COG_PREFIX = "cog/wrf"
    WRF_GEOJSON_PREFIX = "geojson/wrf"

    ECMWF_TP_TILES_PREFIX = "tiles/models/ecmwf/total_precipitation"
    ECMWF_TP_COG_PREFIX = "cog/models/ecmwf/total_precipitation"
    ECMWF_MSLP_COG_PREFIX = "cog/models/ecmwf/mean_sea_level_pressure"
    ECMWF_MSLP_GEOJSON_PREFIX = "geojson/models/ecmwf/mean_sea_level_pressure"

    @staticmethod
    def build_wrf_tile_key(
        product_id: str, init_tag: str, fxxx: str, z: int, x: int, y: int
    ) -> str:
        """Build S3 key for a WRF tile."""
        return f"tiles/wrf/{product_id}/{init_tag}/{fxxx}/{z}/{x}/{y}.webp"

    @staticmethod
    def build_wrf_cog_key(product_id: str, init_tag: str, fxxx: str) -> str:
        """Build S3 key for a WRF primary-field COG."""
        return f"cog/wrf/{product_id}/{init_tag}/{fxxx}.tif"

    @staticmethod
    def build_wrf_geojson_key(
        product_id: str, init_tag: str, fxxx: str, layer: str
    ) -> str:
        """Build S3 key for a WRF GeoJSON layer (barbs, isobars, shear, ...)."""
        return f"geojson/wrf/{product_id}/{init_tag}/{fxxx}/{layer}.json"

    @staticmethod
    def build_wrf_barb_tile_key(
        product_id: str, init_tag: str, fxxx: str, z: int, x: int, y: int
    ) -> str:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Build S3 key for a WRF wind-barb GeoJSON tile (z/x/y)."""
        return f"geojson/wrf/{product_id}/{init_tag}/{fxxx}/barbs/{z}/{x}/{y}.json"

    async def sync_wrf_step_to_redis(
        self,
        redis_client: RedisClient,
        product_id: str,
        init_tag: str,
        fxxx: str,
        tile_ttl: int,
    ) -> int:
        # pylint: disable=too-many-arguments
        """Download all tiles for a WRF forecast step from S3 and store in Redis."""
        s3_prefix = f"{self.WRF_TILES_PREFIX}/{product_id}/{init_tag}/{fxxx}/"
        await self._ensure_connected()
        s3_objects = await self._list_objects(s3_prefix)

        tile_objects = [obj for obj in s3_objects if obj["Key"].endswith(".webp")]
        if not tile_objects:
            return 0

        logger.info(
            "Downloading %d WRF tiles for %s/%s/%s",
            len(tile_objects),
            product_id,
            init_tag,
            fxxx,
        )

        tasks = [
            self._download_wrf_tile_to_redis(
                redis_client, obj["Key"], product_id, init_tag, fxxx, tile_ttl
            )
            for obj in tile_objects
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for r in results if r is True)

    async def _download_wrf_tile_to_redis(
        self,
        redis_client: RedisClient,
        s3_key: str,
        product_id: str,
        init_tag: str,
        fxxx: str,
        tile_ttl: int,
    ) -> bool:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Download a single WRF tile from S3 and store in Redis."""
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

                await redis_client.store_wrf_tile(
                    product_id,
                    init_tag,
                    fxxx,
                    int(z),
                    int(x),
                    int(y),
                    content,
                    ttl=tile_ttl,
                )
                return True
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to download WRF tile %s: %s", s3_key, e)
                return False

    async def list_wrf_layers(
        self, product_id: str, init_tag: str, fxxx: str
    ) -> List[str]:
        """List GeoJSON layer names available under a WRF forecast step.

        Lists with a delimiter: step dirs also hold tiled-overlay subtrees
        (e.g. ``barbs/{z}/{x}/{y}``) whose thousands of keys must not be
        fetched just to be filtered out.
        """
        await self._ensure_connected()
        prefix = f"{self.WRF_GEOJSON_PREFIX}/{product_id}/{init_tag}/{fxxx}/"
        return await self.list_object_basenames(prefix, ".json", delimiter="/")

    async def sync_wrf_geojson_to_redis(
        self,
        redis_client: RedisClient,
        product_id: str,
        init_tag: str,
        fxxx: str,
        layer: str,
        geojson_ttl: int,
    ) -> bool:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Download a single WRF GeoJSON layer from S3 and store it in Redis."""
        if self._client is None:
            raise RuntimeError("S3 client is not connected")

        client = self._client
        s3_key = self.build_wrf_geojson_key(product_id, init_tag, fxxx, layer)
        async with self._semaphore:
            try:
                response = await client.get_object(Bucket=self._bucket, Key=s3_key)
                async with response["Body"] as stream:
                    content = await stream.read()

                await redis_client.store_wrf_geojson(
                    product_id, init_tag, fxxx, layer, content, ttl=geojson_ttl
                )
                return True
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to download WRF GeoJSON %s: %s", s3_key, e)
                return False

    @staticmethod
    def build_ecmwf_tp_tile_key(
        forecast_ts: str, period_ts: str, z: int, x: int, y: int
    ) -> str:
        """Build S3 key for an ECMWF total precipitation tile."""
        return f"tiles/models/ecmwf/total_precipitation/{forecast_ts}/{period_ts}/{z}/{x}/{y}.webp"

    @staticmethod
    def build_ecmwf_tp_cog_key(forecast_ts: str, period_ts: str) -> str:
        """Build S3 key for an ECMWF total precipitation COG."""
        return f"cog/models/ecmwf/total_precipitation/{forecast_ts}/{period_ts}.tif"

    async def sync_ecmwf_tp_period_to_redis(
        self,
        redis_client: RedisClient,
        forecast_ts: str,
        period_ts: str,
        tile_ttl: int,
    ) -> int:
        """Download all tiles for an ECMWF total precipitation period from S3 and store in Redis."""
        s3_prefix = f"{self.ECMWF_TP_TILES_PREFIX}/{forecast_ts}/{period_ts}/"
        await self._ensure_connected()
        s3_objects = await self._list_objects(s3_prefix)

        tile_objects = [obj for obj in s3_objects if obj["Key"].endswith(".webp")]
        if not tile_objects:
            return 0

        logger.info(
            "Downloading %d ECMWF-TP tiles for %s/%s",
            len(tile_objects),
            forecast_ts,
            period_ts,
        )

        tasks = [
            self._download_ecmwf_tp_tile_to_redis(
                redis_client, obj["Key"], forecast_ts, period_ts, tile_ttl
            )
            for obj in tile_objects
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for r in results if r is True)

    async def _download_ecmwf_tp_tile_to_redis(
        self,
        redis_client: RedisClient,
        s3_key: str,
        forecast_ts: str,
        period_ts: str,
        tile_ttl: int,
    ) -> bool:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Download a single ECMWF total precipitation tile from S3 and store in Redis."""
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

                await redis_client.store_ecmwf_tp_tile(
                    forecast_ts,
                    period_ts,
                    int(z),
                    int(x),
                    int(y),
                    content,
                    ttl=tile_ttl,
                )
                return True
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to download ECMWF-TP tile %s: %s", s3_key, e)
                return False

    @staticmethod
    def build_ecmwf_mslp_cog_key(forecast_ts: str, timestamp_ts: str) -> str:
        """Build S3 key for an ECMWF mean sea level pressure COG."""
        return (
            f"cog/models/ecmwf/mean_sea_level_pressure/{forecast_ts}/{timestamp_ts}.tif"
        )

    @staticmethod
    def build_ecmwf_mslp_geojson_key(forecast_ts: str, timestamp_ts: str) -> str:
        """Build S3 key for an ECMWF mean sea level pressure isobars GeoJSON."""
        return f"geojson/models/ecmwf/mean_sea_level_pressure/{forecast_ts}/{timestamp_ts}.json"

    # ============== GFS ==============
    #
    # tiles-processor names every object of a step after the *combined* id
    # `{cycle}_{fxxx}` while nesting it under the cycle, e.g.
    # `.../20260808T0000Z/20260808T0000Z_f003.tif`. The API splits cycle and
    # step into separate path segments, so `_gfs_step_id` is the single place
    # that joins them back.

    @staticmethod
    def _gfs_step_id(cycle: str, fxxx: str) -> str:
        """Object basename tiles-processor writes for one forecast step."""
        return f"{cycle}_{fxxx}"

    @staticmethod
    def gfs_cycle_prefix(s3_segment: str) -> str:
        """Prefix holding every cycle of a product, for listing cycles."""
        return f"tiles/models/gfs/{s3_segment}/"

    @staticmethod
    def gfs_cog_cycle_prefix(s3_segment: str) -> str:
        """Prefix holding every COG of one product, for listing cycles."""
        return f"cog/models/gfs/{s3_segment}/"

    @classmethod
    def build_gfs_tile_key(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        cls, s3_segment: str, cycle: str, fxxx: str, z: int, x: int, y: int
    ) -> str:
        """Build S3 key for a GFS raster tile."""
        step = cls._gfs_step_id(cycle, fxxx)
        return f"tiles/models/gfs/{s3_segment}/{cycle}/{step}/{z}/{x}/{y}.webp"

    @classmethod
    def build_gfs_cog_key(cls, s3_segment: str, cycle: str, fxxx: str) -> str:
        """Build S3 key for a GFS COG (pressure in hPa, or wind speed in kt)."""
        step = cls._gfs_step_id(cycle, fxxx)
        return f"cog/models/gfs/{s3_segment}/{cycle}/{step}.tif"

    @classmethod
    def build_gfs_geojson_key(
        cls, s3_segment: str, cycle: str, fxxx: str, layer: str
    ) -> str:
        """Build S3 key for a single-file GFS overlay (isobars, heights, ...)."""
        step = cls._gfs_step_id(cycle, fxxx)
        return f"geojson/models/gfs/{s3_segment}/{cycle}/{step}_{layer}.json"

    @classmethod
    def build_gfs_barb_tile_key(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        cls, s3_segment: str, cycle: str, fxxx: str, z: int, x: int, y: int
    ) -> str:
        """Build S3 key for one GFS wind-barb GeoJSON tile."""
        step = cls._gfs_step_id(cycle, fxxx)
        return f"geojson/models/gfs/{s3_segment}/{cycle}/{step}_barbs/{z}/{x}/{y}.json"

    async def list_object_basenames(
        self, prefix: str, suffix: str, delimiter: str = ""
    ) -> List[str]:
        """List basenames (without `suffix`) of objects directly under `prefix` ending in `suffix`.

        Example: list_object_basenames("cog/.../{forecast_ts}/", ".tif") yields
        ["20260413T1500Z", "20260413T1800Z", ...].

        Pass ``delimiter="/"`` to exclude deeper sub-prefixes server-side —
        essential when the prefix holds large subtrees (e.g. tiled overlays)
        that would otherwise be fetched and parsed page by page just to be
        filtered out below.
        """
        await self._ensure_connected()
        objects = await self._list_objects(prefix, delimiter=delimiter)
        names: List[str] = []
        for obj in objects:
            key = obj["Key"]
            if not key.endswith(suffix):
                continue
            tail = key[len(prefix) :].lstrip("/")
            # Skip objects that live in deeper sub-prefixes.
            if "/" in tail:
                continue
            names.append(tail[: -len(suffix)])
        return names

    async def sync_ecmwf_mslp_forecast_to_redis(
        self,
        redis_client: RedisClient,
        forecast_ts: str,
        timestamps: List[str],
        geojson_ttl: int,
    ) -> int:
        """Download all GeoJSON files for a forecast from S3 and store in Redis.

        Returns the count of GeoJSONs successfully stored.
        """
        if not timestamps:
            return 0

        await self._ensure_connected()
        logger.info(
            "Downloading %d ECMWF-MSLP GeoJSONs for %s",
            len(timestamps),
            forecast_ts,
        )

        tasks = [
            self._download_ecmwf_mslp_geojson_to_redis(
                redis_client, forecast_ts, timestamp_ts, geojson_ttl
            )
            for timestamp_ts in timestamps
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for r in results if r is True)

    async def _download_ecmwf_mslp_geojson_to_redis(
        self,
        redis_client: RedisClient,
        forecast_ts: str,
        timestamp_ts: str,
        geojson_ttl: int,
    ) -> bool:
        """Download a single MSLP GeoJSON from S3 and store in Redis."""
        if self._client is None:
            raise RuntimeError("S3 client is not connected")

        client = self._client
        s3_key = self.build_ecmwf_mslp_geojson_key(forecast_ts, timestamp_ts)
        async with self._semaphore:
            try:
                response = await client.get_object(Bucket=self._bucket, Key=s3_key)
                async with response["Body"] as stream:
                    content = await stream.read()

                await redis_client.store_ecmwf_mslp_geojson(
                    forecast_ts, timestamp_ts, content, ttl=geojson_ttl
                )
                return True
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to download ECMWF-MSLP GeoJSON %s: %s", s3_key, e)
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
        """Download a single tile from S3. Returns raw bytes or None.

        Infrastructure failures (`BotoCoreError` family, including
        `EndpointConnectionError`, plus socket and timeout errors) degrade
        to ``None`` with a WARNING log so callers can fall back to other
        tiers instead of surfacing a 5xx to the user.
        """
        try:
            client = await self._ensure_connected()
            response = await client.get_object(Bucket=self._bucket, Key=s3_key)
            async with response["Body"] as stream:
                return await stream.read()
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                logger.debug("S3 tile miss: %s", s3_key)
                return None
            logger.warning("S3 error for tile %s: %s", s3_key, exc)
            return None
        except (BotoCoreError, asyncio.TimeoutError, OSError) as exc:
            logger.warning("S3 unavailable for tile %s: %s", s3_key, exc)
            return None

    @staticmethod
    def build_basemap_tile_key(provider_id: str, z: int, x: int, y: int) -> str:
        """Build S3 key for a base map tile."""
        return f"basemap/{provider_id}/{z}/{x}/{y}.png"

    async def upload_tile(
        self, key: str, data: bytes, content_type: str = "image/png"
    ) -> None:
        """Upload a tile to S3."""
        client = await self._ensure_connected()
        async with self._semaphore:
            await client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )

    async def ensure_lifecycle_expiration(
        self, days: int, rule_id: str = "basemap-tile-expiration", prefix: str = ""
    ) -> bool:
        """
        Idempotently set a lifecycle rule that expires objects under `prefix`
        after `days` days from their creation (i.e. last PUT).

        `prefix` defaults to ``""`` (bucket-wide). Callers that only want a
        rolling subset expired — e.g. the weather-stations scraper, whose
        registry/`latest` singletons must NOT be swept — pass the rolling
        prefix so the always-present objects survive.

        Safe to call on every startup, but note the S3 semantics:
        `put_bucket_lifecycle_configuration` replaces the bucket's ENTIRE rule
        set, not just the rule named `rule_id`. One caller per bucket is
        therefore a hard requirement — a second caller would silently drop the
        first one's rule. Not all S3-compatible backends support this API;
        failures are logged at WARNING and do not abort startup.

        Returns ``True`` when the policy was applied, ``False`` on a swallowed
        error. Callers that want "retry until it sticks" semantics (the
        basemap scraper) poll the return value.
        """
        client = await self._ensure_connected()
        try:
            await client.put_bucket_lifecycle_configuration(
                Bucket=self._bucket,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": rule_id,
                            "Status": "Enabled",
                            "Filter": {"Prefix": prefix},
                            "Expiration": {"Days": days},
                        }
                    ]
                },
            )
            logger.info(
                "S3 lifecycle policy set on bucket %s: expire objects under %r "
                "after %d days",
                self._bucket,
                prefix,
                days,
            )
            return True
        except (ClientError, BotoCoreError, asyncio.TimeoutError, OSError) as exc:
            # Lifecycle config is best-effort: a missing policy never blocks
            # tile serving, so an unreachable or non-supporting backend should
            # degrade to a warning rather than aborting startup.
            logger.warning(
                "Could not set lifecycle policy on bucket %s "
                "(backend unreachable or unsupported): %s",
                self._bucket,
                exc,
            )
            return False

    async def has_any_object(self, prefix: str) -> bool:
        """True iff at least one object was positively observed under the prefix.

        Infrastructure failures (`BotoCoreError`/`ClientError`/socket/timeout)
        are degraded to ``False`` with a WARNING log: callers (provider
        availability checks) interpret "no observed tile" the same way as
        "tile genuinely absent", and an S3 outage should not take the
        listing endpoint down when upstream is still serving.
        """
        try:
            client = await self._ensure_connected()
            response = await client.list_objects_v2(
                Bucket=self._bucket, Prefix=prefix, MaxKeys=1
            )
            return bool(response.get("Contents"))
        except (ClientError, BotoCoreError, asyncio.TimeoutError, OSError) as exc:
            logger.warning(
                "S3 unavailable while checking prefix %s; treating as empty: %s",
                prefix,
                exc,
            )
            return False

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

    async def ensure_bucket(self) -> None:
        """Create the configured bucket if it doesn't already exist.

        Idempotent: a present bucket is a no-op; absence triggers create_bucket.
        Any other error (auth, network) propagates so the caller can fail fast.
        """
        client = await self._ensure_connected()
        try:
            await client.head_bucket(Bucket=self._bucket)
            return
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in ("404", "NoSuchBucket", "NotFound"):
                raise
        await client.create_bucket(Bucket=self._bucket)
        logger.info("Created S3 bucket %s", self._bucket)

    async def is_reachable(self) -> bool:
        """Return True if the S3 endpoint answers at all, regardless of whether
        the bucket exists or we're authorized.

        Only network-level failures count as unreachable, so a fresh S3 with no
        buckets yet still reports reachable (unlike `check_connection`, which
        probes a specific bucket and returns False on a 404).
        """
        try:
            client = await self._ensure_connected()
            await client.list_buckets()
            return True
        except ClientError:
            return True  # endpoint answered (auth/permission) -> reachable
        except (BotoCoreError, OSError) as exc:
            logger.debug("S3 endpoint not reachable yet: %s", exc)
            return False

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

    async def delete_object(self, key: str) -> bool:
        """Delete a single object. Returns True on success, False on error."""
        client = await self._ensure_connected()
        try:
            await client.delete_object(Bucket=self._bucket, Key=key)
            return True
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Error deleting object %s: %s", key, exc)
            return False

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
