"""Strategies for point-value sampling from remote COG files."""

import asyncio
import math
from typing import Optional, Protocol

import rasterio
from rasterio.errors import RasterioIOError
from rasterio.windows import Window

from clients.s3_client import S3Client
from dependencies import logger, settings


class NoS3ClientConfiguredError(Exception):
    """Raised when point-value strategy is created without an S3 client."""


class CogObjectNotFoundError(Exception):
    """Raised when the target COG object does not exist in S3."""


class NoDataOrOutsideSampleError(Exception):
    """Raised when point sampling resolves to nodata or out-of-bounds."""


class PointValueStrategy(Protocol):
    """Protocol for point-value sampling strategies."""

    async def sample_cog_value(self, cog_key: str, lat: float, lon: float) -> float:
        """Sample a single value from a COG object path."""


class S3CogPointValueStrategy:
    """Point-value strategy backed by S3 object checks and rasterio VSI reads."""

    def __init__(self, s3_client: Optional[S3Client]):
        if s3_client is None:
            raise NoS3ClientConfiguredError(
                "S3CogPointValueStrategy requires a configured S3Client"
            )
        self._s3_client = s3_client

    async def sample_cog_value(self, cog_key: str, lat: float, lon: float) -> float:
        """Validate COG existence and sample nearest value from raster."""
        if not await self._s3_client.object_exists(cog_key):
            raise CogObjectNotFoundError()

        return await asyncio.to_thread(self._read_point_from_cog_sync, cog_key, lat, lon)

    def _read_point_from_cog_sync(self, cog_key: str, lat: float, lon: float) -> float:
        """Open remote COG and sample nearest value. Runs in thread."""
        vsi_path = f"/vsis3/{settings.s3_tiles_data_bucket_name}/{cog_key}"

        try:
            with rasterio.open(vsi_path) as dataset:
                bounds = dataset.bounds
                if (
                    lon < bounds.left
                    or lon > bounds.right
                    or lat < bounds.bottom
                    or lat > bounds.top
                ):
                    raise NoDataOrOutsideSampleError()

                row, col = dataset.index(lon, lat)
                if row < 0 or col < 0 or row >= dataset.height or col >= dataset.width:
                    raise NoDataOrOutsideSampleError()

                pixel = dataset.read(1, window=Window(col, row, 1, 1))
                value = float(pixel[0, 0])

                if self._is_nodata(value, dataset.nodata):
                    raise NoDataOrOutsideSampleError()

                return value
        except RasterioIOError as exc:
            logger.error("Failed reading COG point from %s: %s", vsi_path, exc)
            raise NoDataOrOutsideSampleError() from exc

    @staticmethod
    def _is_nodata(value: float, nodata: Optional[float]) -> bool:
        """Determine whether sampled value is nodata/invalid."""
        if math.isnan(value):
            return True
        if nodata is None:
            return False
        try:
            return value == float(nodata)
        except (TypeError, ValueError):
            return False
