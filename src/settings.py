"""Configuration settings for the application."""

import json
import os
from pathlib import Path

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

    # --- Operational tuning (loaded from settings.json, env overrides) ---
    sync_mode: str
    tile_ttl: int
    tileset_listing_ttl: int
    sync_interval_seconds: int
    radar_sync_interval_seconds: int
    cache_control_config: str
    cache_control_tile: str
    # File locks used by sync services so that only one uvicorn worker
    # runs the background sync task (fcntl exclusive lock).
    sync_lock_path: str = "/tmp/sync.lock"
    radar_lock_path: str = "/tmp/radar_sync.lock"
    s3_max_concurrent_downloads: int = 5

    def __init__(self):
        settings_json_path = Path(__file__).resolve().parent.parent / "settings.json"

        self._load_from_json(settings_json_path)
        self._load_from_env()

    def _load_from_json(self, settings_json_path: Path) -> None:
        """Load operational settings from settings.json."""
        if not settings_json_path.is_file():
            return

        with open(settings_json_path, encoding="utf-8") as f:
            data = json.load(f)

        json_keys = {
            "sync_mode",
            "tile_ttl",
            "tileset_listing_ttl",
            "sync_interval_seconds",
            "radar_sync_interval_seconds",
            "cache_control_config",
            "cache_control_tile",
            "s3_max_concurrent_downloads",
        }

        for key in json_keys:
            if key in data:
                setattr(self, key, data[key])

    @staticmethod
    def _env_int(key: str, default: int) -> int:
        """Read an env var as int, falling back to default if unset or empty."""
        value = os.getenv(key, "")
        return int(value) if value else default

    def _load_from_env(self) -> None:
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

        # Operational (env overrides JSON)
        self.sync_interval_seconds = self._env_int(
            "SYNC_INTERVAL_SECONDS", self.sync_interval_seconds
        )
        self.radar_sync_interval_seconds = self._env_int(
            "RADAR_SYNC_INTERVAL_SECONDS", self.radar_sync_interval_seconds
        )
        self.sync_mode = os.getenv("SYNC_MODE", self.sync_mode) or self.sync_mode
        self.tile_ttl = self._env_int("TILE_TTL", self.tile_ttl)
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
