"""Point-query service to read single values from remote COG files."""

from dataclasses import dataclass
from typing import Optional

from clients.s3_client import S3Client
from services.base_service import BaseProductService
from services.gfs_config import get_product as get_gfs_product
from services.gfs_config import secondary_unit as gfs_secondary_unit
from services.point_value_strategy import (
    CogObjectNotFoundError,
    NoDataOrOutsideSampleError,
    PointValueStrategy,
)


class PointValueError(Exception):
    """Base domain error for point-value operations."""


class CogNotFoundError(PointValueError):
    """Raised when the target COG object does not exist."""


class NoDataOrOutsideError(PointValueError):
    """Raised when the point falls outside raster bounds or resolves to nodata."""


@dataclass(frozen=True)
class PointSample:
    """Point sample extracted from a COG file."""

    value: float
    unit: str


class PointValueService(BaseProductService):
    """Service responsible for sampling point values from COG objects."""

    def __init__(self) -> None:
        self._strategy: Optional[PointValueStrategy] = None

    SATELLITE_UNITS = {
        "band_13": "K",
        "band_9": "K",
        "band_2": "1",
        "glm_fed": "flashes/min",
        "glm_toe": "fJ",
        "glm_mfa": "km2",
    }

    RADAR_UNITS = {
        "DBZH": "dBZ",
        # Long-range reflectivity (subvolume 04): same moment as DBZH.
        "DBZH_450KM": "dBZ",
        "VRAD": "m/s",
    }

    MODEL_UNITS = {
        "ecmwf_total_precipitation": "mm",
        "ecmwf_mean_sea_level_pressure": "hPa",
    }

    WRF_PRODUCT_UNITS = {
        "Colmax": "dBZ",
        "Rafagas": "kt",
        "Campo900hPa": "g/kg",
        "Precipitacion1h": "mm",
        "MUCAPE": "J/kg",
        "AguaPrecipitable": "mm",
        "JetCapasBajas": "kt",
        "CortanteNivelesBajos": "kt",
        "CAPE_BRN": "J/kg",
        "Granizo": "",
    }

    # Units for WRF secondary point-query variables (keyed by variable name,
    # matching the secondary COG suffix produced by tiles-processor).
    WRF_SECONDARY_UNITS = {
        "wind": "kt",
        "slp": "hPa",
        "shear_850_500": "kt",
        "shear_850_700": "kt",
        "brn": "",
        "haildiammax": "cm",
    }

    def set_strategy(self, strategy: PointValueStrategy) -> None:
        """Set point-value strategy (called during app startup)."""
        self._strategy = strategy

    async def sample_satellite_point(
        self,
        band_id: str,
        tileset_id: str,
        lat: float,
        lon: float,
    ) -> PointSample:
        """Sample a satellite COG at a specific coordinate."""
        cog_key = f"cog/{band_id}/{tileset_id}.tif"
        unit = self.SATELLITE_UNITS.get(band_id, "1")
        value = await self._sample_value(cog_key, lat, lon)
        return PointSample(value=value, unit=unit)

    async def sample_radar_point(
        self,
        radar_id: str,
        variable_id: str,
        elevation_id: str,
        tileset_id: str,
        lat: float,
        lon: float,
    ) -> PointSample:
        """Sample a radar COG at a specific coordinate."""
        cog_key = f"cog/radar/{radar_id}/{variable_id}/{elevation_id}/{tileset_id}.tif"
        unit = self.RADAR_UNITS.get(variable_id, "1")
        value = await self._sample_value(cog_key, lat, lon)
        return PointSample(value=value, unit=unit)

    async def sample_ecmwf_tp_point(
        self,
        forecast_ts: str,
        period_ts: str,
        lat: float,
        lon: float,
    ) -> PointSample:
        """Sample an ECMWF total precipitation COG at a specific coordinate."""
        cog_key = f"cog/models/ecmwf/total_precipitation/{forecast_ts}/{period_ts}.tif"
        unit = self.MODEL_UNITS.get("ecmwf_total_precipitation", "1")
        value = await self._sample_value(cog_key, lat, lon)
        return PointSample(value=value, unit=unit)

    async def sample_wrf_point(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        lat: float,
        lon: float,
    ) -> PointSample:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Sample a WRF primary-field COG at a specific coordinate."""
        cog_key = f"cog/wrf/{product_id}/{init_tag}/{fxxx}.tif"
        unit = self.WRF_PRODUCT_UNITS.get(product_id, "")
        value = await self._sample_value(cog_key, lat, lon)
        return PointSample(value=value, unit=unit)

    async def sample_wrf_secondary_point(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        variable: str,
        lat: float,
        lon: float,
    ) -> PointSample:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Sample a WRF secondary-variable COG (wind / contour) at a point."""
        cog_key = f"cog/wrf/{product_id}/{init_tag}/{fxxx}.{variable}.tif"
        unit = self.WRF_SECONDARY_UNITS.get(variable, "")
        value = await self._sample_value(cog_key, lat, lon)
        return PointSample(value=value, unit=unit)

    async def sample_ecmwf_mslp_point(
        self,
        forecast_ts: str,
        timestamp_ts: str,
        lat: float,
        lon: float,
    ) -> PointSample:
        """Sample an ECMWF mean sea level pressure COG at a specific coordinate."""
        cog_key = (
            f"cog/models/ecmwf/mean_sea_level_pressure/"
            f"{forecast_ts}/{timestamp_ts}.tif"
        )
        unit = self.MODEL_UNITS.get("ecmwf_mean_sea_level_pressure", "1")
        value = await self._sample_value(cog_key, lat, lon)
        return PointSample(value=value, unit=unit)

    async def sample_gfs_point(
        self,
        product_id: str,
        cycle: str,
        fxxx: str,
        lat: float,
        lon: float,
    ) -> PointSample:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Sample a GFS COG at a coordinate."""
        product = get_gfs_product(product_id)
        if product is None:
            raise CogNotFoundError()
        cog_key = S3Client.build_gfs_cog_key(product.s3_segment, cycle, fxxx)
        value = await self._sample_value(cog_key, lat, lon)
        return PointSample(value=value, unit=product.unit)

    async def sample_gfs_secondary_point(
        self,
        product_id: str,
        cycle: str,
        fxxx: str,
        variable: str,
        lat: float,
        lon: float,
    ) -> PointSample:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Sample a GFS secondary-variable COG (thickness / temperature / height).

        The unit lookup doubles as validation: `variable` arrives as a URL
        segment and is interpolated into an S3 key, so an unrecognised one is
        rejected here rather than turned into a request for an arbitrary object.
        """
        product = get_gfs_product(product_id)
        unit = gfs_secondary_unit(product_id, variable)
        if product is None or unit is None:
            raise CogNotFoundError()
        cog_key = S3Client.build_gfs_secondary_cog_key(
            product.s3_segment, cycle, variable, fxxx
        )
        value = await self._sample_value(cog_key, lat, lon)
        return PointSample(value=value, unit=unit)

    async def _sample_value(self, cog_key: str, lat: float, lon: float) -> float:
        strategy = self._get_strategy()
        try:
            return await strategy.sample_cog_value(cog_key, lat, lon)
        except CogObjectNotFoundError as exc:
            raise CogNotFoundError() from exc
        except NoDataOrOutsideSampleError as exc:
            raise NoDataOrOutsideError() from exc

    def _get_strategy(self) -> PointValueStrategy:
        if self._strategy is None:
            raise RuntimeError("PointValueService strategy is not configured")
        return self._strategy


point_value_service = PointValueService()
