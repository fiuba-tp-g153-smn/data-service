import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    """
    Application settings management using Pydantic.
    Handles configuration loading for the application from environment variables.
    """

    log_level: str = ""
    app_env: str = ""

    # S3 Tiles Data Configuration
    s3_tiles_data_endpoint: str = ""
    s3_tiles_data_access_key: str = ""
    s3_tiles_data_secret_key: str = ""
    s3_tiles_data_bucket_name: str = "tiles-data"
    s3_tiles_data_secure: bool = False
    sync_interval_seconds: int = 60

    # Caching
    cache_control_config: str = "public, max-age=60, stale-while-revalidate=300"
    cache_control_tile: str = "public, max-age=31536000, immutable"

    def __init__(self):
        # Load from environment variables
        self._load_from_env()

    def _load_from_env(self) -> None:
        """Load all configuration values directly from environment variables."""
        self.log_level = os.getenv("LOG_LEVEL", self.log_level)
        self.app_env = os.getenv("APP_ENV", self.app_env)

        # S3 Tiles Data Configuration
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
        self.sync_interval_seconds = int(
            os.getenv("SYNC_INTERVAL_SECONDS", str(self.sync_interval_seconds))
        )

        # Caching
        self.cache_control_config = os.getenv(
            "CACHE_CONTROL_CONFIG", self.cache_control_config
        )
        self.cache_control_tile = os.getenv(
            "CACHE_CONTROL_TILE", self.cache_control_tile
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
