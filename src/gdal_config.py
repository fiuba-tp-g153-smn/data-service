"""GDAL/VSI runtime configuration for reading remote COG files over S3."""

import os

from dependencies import logger, settings


def configure_gdal_vsi_s3() -> None:
    """Configure GDAL environment variables for efficient VSI S3 range reads."""
    if not settings.is_s3_configured():
        logger.warning("Skipping GDAL VSI S3 configuration: S3 is not fully configured")
        return

    endpoint = settings.s3_tiles_data_endpoint.strip()
    endpoint = endpoint.replace("https://", "").replace("http://", "")

    os.environ["AWS_S3_ENDPOINT"] = endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = settings.s3_tiles_data_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.s3_tiles_data_secret_key
    os.environ["AWS_HTTPS"] = "YES" if settings.s3_tiles_data_secure else "NO"
    os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"

    # Reduce extra directory/network calls on COG open, keep reads range-friendly.
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = settings.gdal_disable_readdir_on_open
    os.environ["CPL_VSIL_CURL_USE_HEAD"] = settings.gdal_curl_use_head
    os.environ["VSI_CACHE"] = "TRUE" if settings.gdal_vsi_cache else "FALSE"
    os.environ["VSI_CACHE_SIZE"] = settings.gdal_vsi_cache_size
    os.environ["CPL_VSIL_CURL_CACHE_SIZE"] = settings.gdal_vsicurl_cache_size

    logger.info(
        "Configured GDAL VSI S3 environment (endpoint=%s, https=%s)",
        endpoint,
        os.environ["AWS_HTTPS"],
    )
