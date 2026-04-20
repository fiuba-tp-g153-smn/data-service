"""Configuration settings for the application."""

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

load_dotenv()


class Settings:
    # pylint: disable=too-many-instance-attributes
    """
    Application settings management.

    Loads tunable operational settings from settings.json, then lets
    environment variables override any value for deployment flexibility.
    Secrets and infrastructure URLs are loaded only from env vars.
    """

    # --- Env-only: secrets & infrastructure ---
    log_level: str = ""
    app_env: str = ""
    s3_tiles_data_endpoint: str = ""
    s3_tiles_data_access_key: str = ""
    s3_tiles_data_secret_key: str = ""
    s3_tiles_data_bucket_name: str = ""
    s3_tiles_data_secure: bool = False
    redis_url: str = ""
    gdal_disable_readdir_on_open: str = "EMPTY_DIR"
    gdal_curl_use_head: str = "NO"
    gdal_vsi_cache: bool = True
    gdal_vsi_cache_size: str = "50MB"
    gdal_vsicurl_cache_size: str = "200MB"

    # --- Operational tuning (loaded from settings.json, env overrides) ---
    sync_mode: str
    tile_ttl: int
    radar_tile_ttl: int
    tileset_listing_ttl: int
    sync_interval_seconds: int
    radar_sync_interval_seconds: int
    cache_control_config: str
    cache_control_tile: str
    # File locks used by sync services so that only one uvicorn worker
    # runs the background sync task (fcntl exclusive lock).
    sync_lock_path: str = "/tmp/sync.lock"
    radar_lock_path: str = "/tmp/radar_sync.lock"
    ecmwf_lock_path: str = "/tmp/ecmwf_sync.lock"
    s3_max_concurrent_downloads: int = 5
    # ECMWF precipitation (loaded from settings.json, env overrides)
    ecmwf_tile_ttl: int
    ecmwf_forecasts_to_keep: int
    ecmwf_sync_interval_seconds: int
    # Basemap scraper (loaded from settings.json, env overrides)
    basemap_tile_ttl: int = 604800
    basemap_scrape_interval_seconds: int = 604800
    basemap_scrape_concurrent: int = 20
    basemap_scrape_delay_ms: int = 30
    basemap_cache_max_zoom: int = 11
    basemap_cache_concurrent: int = 10
    basemap_scrape_lock_path: str = "/tmp/basemap_scrape.lock"
    s3_basemap_bucket_name: str = "basemap-tiles"
    basemap_providers: List[Dict[str, Any]] = []
    # Basemap tile bounding box (Argentina + surrounding region by default)
    basemap_bbox_lat_min: float = -60.0
    basemap_bbox_lat_max: float = -15.0
    basemap_bbox_lon_min: float = -85.0
    basemap_bbox_lon_max: float = -40.0
    # Basemap HTTP client tuning
    basemap_http_timeout_seconds: int = 10
    basemap_http_max_retries: int = 3
    # S3 bucket lifecycle: auto-expire tiles after N days. Scrape cadence (7d by
    # default) must be strictly less than this TTL so weekly re-uploads refresh
    # object age before S3 expires them.
    basemap_s3_object_ttl_days: int = 14
    # When False, disables tier-3 provider proxy — service serves only from
    # Redis/S3 (fully offline reads; scraper still pulls from providers).
    basemap_online_fallback_enabled: bool = True
    # TTL for the Redis-backed cache of per-provider "has any tile in S3?"
    # answers used by /basemap/providers when the online fallback is disabled.
    basemap_provider_presence_ttl: int = 60
    # Resumable-scrape state (SQLite cold storage + checkpoint knobs)
    basemap_scrape_state_db_path: str = "data/basemap_scraper_state.sqlite"
    basemap_scrape_checkpoint_every: int = 200
    basemap_scrape_checkpoint_seconds: float = 5.0
    # Redis-backed negative cache for tiles that miss both S3 and the relay.
    # Short TTL is a trade-off: long enough to suppress concurrent-request
    # storms and repeat probes; short enough that a freshly-scraped tile is
    # unmasked quickly even if the scraper-side invalidation hook is bypassed.
    basemap_negative_cache_enabled: bool = True
    basemap_negative_cache_ttl: int = 300
    # Reader-path HTTP client (isolated from the scraper's pool). Tight budget
    # so user requests can't queue behind the scraper's retry loop.
    basemap_reader_http_concurrent: int = 8
    basemap_reader_http_timeout_seconds: int = 3
    basemap_reader_http_max_retries: int = 1
    # Hard wall-clock deadline per tile request. Bounds single-flight waiters
    # and the relay fallback together.
    basemap_request_deadline_seconds: float = 4.0
    # Cache-Control header returned for missing tiles (datalayer-style: a
    # transparent PNG with a short TTL so the browser stops re-requesting).
    basemap_cache_control_tile_miss: str = "public, max-age=300, immutable"
    # Per-domain sync mode for basemap, independent of `sync_mode`. One of
    # "full" (scraper on + Redis cache on), "on_demand" (scraper off, Redis
    # populated lazily on cold reads), "no_cache" (scraper off, reader
    # streams straight from S3/relay — nothing lands in Redis).
    basemap_sync_mode: str = "full"

    _BASEMAP_SYNC_MODES = ("full", "on_demand", "no_cache", "relay_only")

    def __init__(self):
        settings_json_path = Path(__file__).resolve().parent.parent / "settings.json"

        self._load_from_json(settings_json_path)
        self._load_from_env()
        self._validate()

    def _load_from_json(self, settings_json_path: Path) -> None:
        """Load operational settings from settings.json."""
        if not settings_json_path.is_file():
            return

        with open(settings_json_path, encoding="utf-8") as f:
            data = json.load(f)

        json_keys = {
            "sync_mode",
            "tile_ttl",
            "radar_tile_ttl",
            "tileset_listing_ttl",
            "sync_interval_seconds",
            "radar_sync_interval_seconds",
            "cache_control_config",
            "cache_control_tile",
            "s3_max_concurrent_downloads",
            "ecmwf_tile_ttl",
            "ecmwf_forecasts_to_keep",
            "ecmwf_sync_interval_seconds",
            "basemap_tile_ttl",
            "basemap_scrape_interval_seconds",
            "basemap_scrape_concurrent",
            "basemap_scrape_delay_ms",
            "basemap_cache_max_zoom",
            "basemap_cache_concurrent",
            "basemap_providers",
            "basemap_bbox_lat_min",
            "basemap_bbox_lat_max",
            "basemap_bbox_lon_min",
            "basemap_bbox_lon_max",
            "basemap_http_timeout_seconds",
            "basemap_http_max_retries",
            "basemap_s3_object_ttl_days",
            "basemap_online_fallback_enabled",
            "basemap_provider_presence_ttl",
            "basemap_scrape_state_db_path",
            "basemap_scrape_checkpoint_every",
            "basemap_scrape_checkpoint_seconds",
            "basemap_negative_cache_enabled",
            "basemap_negative_cache_ttl",
            "basemap_reader_http_concurrent",
            "basemap_reader_http_timeout_seconds",
            "basemap_reader_http_max_retries",
            "basemap_request_deadline_seconds",
            "basemap_cache_control_tile_miss",
            "basemap_sync_mode",
        }

        for key in json_keys:
            if key in data:
                setattr(self, key, data[key])

    @staticmethod
    def _env_int(key: str, default: int) -> int:
        """Read an env var as int, falling back to default if unset or empty."""
        value = os.getenv(key, "")
        return int(value) if value else default

    @staticmethod
    def _env_float(key: str, default: float) -> float:
        """Read an env var as float, falling back to default if unset or empty."""
        value = os.getenv(key, "")
        return float(value) if value else default

    @staticmethod
    def _env_bool(key: str, default: bool) -> bool:
        """Read an env var as bool (truthy: 1/true/yes), falling back to default."""
        value = os.getenv(key, "")
        if not value:
            return default
        return value.strip().lower() in ("1", "true", "yes", "on")

    def _load_from_env(self) -> None:
        # pylint: disable=too-many-statements
        """Load from environment variables (overrides JSON values)."""
        self.log_level = os.getenv("LOG_LEVEL", self.log_level)
        self.app_env = os.getenv("APP_ENV", self.app_env)

        # S3
        self.s3_tiles_data_endpoint = os.getenv(
            "S3_TILES_DATA_ENDPOINT", self.s3_tiles_data_endpoint
        )
        self.s3_tiles_data_access_key = os.getenv(
            "S3_TILES_DATA_ACCESS_KEY", self.s3_tiles_data_access_key
        )
        self.s3_tiles_data_secret_key = os.getenv(
            "S3_TILES_DATA_SECRET_KEY", self.s3_tiles_data_secret_key
        )
        self.s3_tiles_data_bucket_name = os.getenv(
            "S3_TILES_DATA_BUCKET_NAME", self.s3_tiles_data_bucket_name
        )
        self.s3_tiles_data_secure = (
            os.getenv("S3_TILES_DATA_SECURE", "false").lower() == "true"
        )

        # Redis
        self.redis_url = os.getenv("REDIS_URL", self.redis_url)

        # GDAL / VSI S3 tuning
        self.gdal_disable_readdir_on_open = os.getenv(
            "GDAL_DISABLE_READDIR_ON_OPEN", self.gdal_disable_readdir_on_open
        )
        self.gdal_curl_use_head = os.getenv(
            "CPL_VSIL_CURL_USE_HEAD", self.gdal_curl_use_head
        )
        self.gdal_vsi_cache = os.getenv("VSI_CACHE", "TRUE").upper() == "TRUE"
        self.gdal_vsi_cache_size = os.getenv("VSI_CACHE_SIZE", self.gdal_vsi_cache_size)
        self.gdal_vsicurl_cache_size = os.getenv(
            "CPL_VSIL_CURL_CACHE_SIZE", self.gdal_vsicurl_cache_size
        )

        # Operational (env overrides JSON)
        self.sync_interval_seconds = self._env_int(
            "SYNC_INTERVAL_SECONDS", self.sync_interval_seconds
        )
        self.radar_sync_interval_seconds = self._env_int(
            "RADAR_SYNC_INTERVAL_SECONDS", self.radar_sync_interval_seconds
        )
        self.sync_mode = os.getenv("SYNC_MODE", self.sync_mode) or self.sync_mode
        self.tile_ttl = self._env_int("TILE_TTL", self.tile_ttl)
        self.radar_tile_ttl = self._env_int("RADAR_TILE_TTL", self.radar_tile_ttl)
        self.tileset_listing_ttl = self._env_int(
            "TILESET_LISTING_TTL", self.tileset_listing_ttl
        )
        self.cache_control_config = os.getenv(
            "CACHE_CONTROL_CONFIG", self.cache_control_config
        )
        self.cache_control_tile = os.getenv(
            "CACHE_CONTROL_TILE", self.cache_control_tile
        )
        self.s3_max_concurrent_downloads = self._env_int(
            "S3_MAX_CONCURRENT_DOWNLOADS", self.s3_max_concurrent_downloads
        )
        self.ecmwf_tile_ttl = self._env_int("ECMWF_TILE_TTL", self.ecmwf_tile_ttl)
        self.ecmwf_forecasts_to_keep = self._env_int(
            "ECMWF_FORECASTS_TO_KEEP", self.ecmwf_forecasts_to_keep
        )
        self.ecmwf_sync_interval_seconds = self._env_int(
            "ECMWF_SYNC_INTERVAL_SECONDS", self.ecmwf_sync_interval_seconds
        )

        # Basemap
        self.s3_basemap_bucket_name = os.getenv(
            "S3_BASEMAP_BUCKET_NAME", self.s3_basemap_bucket_name
        )
        self.basemap_tile_ttl = self._env_int("BASEMAP_TILE_TTL", self.basemap_tile_ttl)
        self.basemap_scrape_interval_seconds = self._env_int(
            "BASEMAP_SCRAPE_INTERVAL_SECONDS", self.basemap_scrape_interval_seconds
        )
        self.basemap_scrape_concurrent = self._env_int(
            "BASEMAP_SCRAPE_CONCURRENT", self.basemap_scrape_concurrent
        )
        self.basemap_scrape_delay_ms = self._env_int(
            "BASEMAP_SCRAPE_DELAY_MS", self.basemap_scrape_delay_ms
        )
        self.basemap_cache_max_zoom = self._env_int(
            "BASEMAP_CACHE_MAX_ZOOM", self.basemap_cache_max_zoom
        )
        self.basemap_cache_concurrent = self._env_int(
            "BASEMAP_CACHE_CONCURRENT", self.basemap_cache_concurrent
        )
        self.basemap_bbox_lat_min = self._env_float(
            "BASEMAP_BBOX_LAT_MIN", self.basemap_bbox_lat_min
        )
        self.basemap_bbox_lat_max = self._env_float(
            "BASEMAP_BBOX_LAT_MAX", self.basemap_bbox_lat_max
        )
        self.basemap_bbox_lon_min = self._env_float(
            "BASEMAP_BBOX_LON_MIN", self.basemap_bbox_lon_min
        )
        self.basemap_bbox_lon_max = self._env_float(
            "BASEMAP_BBOX_LON_MAX", self.basemap_bbox_lon_max
        )
        self.basemap_http_timeout_seconds = self._env_int(
            "BASEMAP_HTTP_TIMEOUT_SECONDS", self.basemap_http_timeout_seconds
        )
        self.basemap_http_max_retries = self._env_int(
            "BASEMAP_HTTP_MAX_RETRIES", self.basemap_http_max_retries
        )
        self.basemap_s3_object_ttl_days = self._env_int(
            "BASEMAP_S3_OBJECT_TTL_DAYS", self.basemap_s3_object_ttl_days
        )
        self.basemap_online_fallback_enabled = self._env_bool(
            "BASEMAP_ONLINE_FALLBACK_ENABLED", self.basemap_online_fallback_enabled
        )
        self.basemap_provider_presence_ttl = self._env_int(
            "BASEMAP_PROVIDER_PRESENCE_TTL", self.basemap_provider_presence_ttl
        )
        self.basemap_scrape_state_db_path = os.getenv(
            "BASEMAP_SCRAPE_STATE_DB_PATH", self.basemap_scrape_state_db_path
        )
        self.basemap_scrape_checkpoint_every = self._env_int(
            "BASEMAP_SCRAPE_CHECKPOINT_EVERY", self.basemap_scrape_checkpoint_every
        )
        self.basemap_scrape_checkpoint_seconds = self._env_float(
            "BASEMAP_SCRAPE_CHECKPOINT_SECONDS", self.basemap_scrape_checkpoint_seconds
        )
        self.basemap_negative_cache_enabled = self._env_bool(
            "BASEMAP_NEGATIVE_CACHE_ENABLED", self.basemap_negative_cache_enabled
        )
        self.basemap_negative_cache_ttl = self._env_int(
            "BASEMAP_NEGATIVE_CACHE_TTL", self.basemap_negative_cache_ttl
        )
        self.basemap_reader_http_concurrent = self._env_int(
            "BASEMAP_READER_HTTP_CONCURRENT", self.basemap_reader_http_concurrent
        )
        self.basemap_reader_http_timeout_seconds = self._env_int(
            "BASEMAP_READER_HTTP_TIMEOUT_SECONDS",
            self.basemap_reader_http_timeout_seconds,
        )
        self.basemap_reader_http_max_retries = self._env_int(
            "BASEMAP_READER_HTTP_MAX_RETRIES", self.basemap_reader_http_max_retries
        )
        self.basemap_request_deadline_seconds = self._env_float(
            "BASEMAP_REQUEST_DEADLINE_SECONDS", self.basemap_request_deadline_seconds
        )
        self.basemap_cache_control_tile_miss = os.getenv(
            "BASEMAP_CACHE_CONTROL_TILE_MISS", self.basemap_cache_control_tile_miss
        )
        self.basemap_sync_mode = (
            os.getenv("BASEMAP_SYNC_MODE", self.basemap_sync_mode)
            or self.basemap_sync_mode
        )

    def _validate(self) -> None:
        """Fail-fast validation for values with a fixed domain."""
        if self.basemap_sync_mode not in self._BASEMAP_SYNC_MODES:
            raise ValueError(
                f"Invalid basemap_sync_mode={self.basemap_sync_mode!r}; "
                f"expected one of {self._BASEMAP_SYNC_MODES}"
            )
        if (
            self.basemap_sync_mode == "relay_only"
            and not self.basemap_online_fallback_enabled
        ):
            raise ValueError(
                "Invalid combination: basemap_sync_mode='relay_only' requires "
                "basemap_online_fallback_enabled=true; otherwise the service "
                "has no source of tile data."
            )

    def is_s3_configured(self) -> bool:
        """Check if S3 is properly configured."""
        return bool(
            self.s3_tiles_data_endpoint
            and self.s3_tiles_data_access_key
            and self.s3_tiles_data_secret_key
        )

    @staticmethod
    def get_settings() -> "Settings":
        """Factory method to create and return a Settings instance."""
        return Settings()
