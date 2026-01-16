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

    # MinIO Configuration
    minio_endpoint: str = ""
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_bucket: str = "tiles-data"
    minio_secure: bool = False
    sync_interval_seconds: int = 60

    def __init__(self):
        # Load from environment variables
        self._load_from_env()

    def _load_from_env(self) -> None:
        """Load all configuration values directly from environment variables."""
        self.log_level = os.getenv("LOG_LEVEL", self.log_level)
        self.app_env = os.getenv("APP_ENV", self.app_env)

        # MinIO Configuration
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", self.minio_endpoint)
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", self.minio_access_key)
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", self.minio_secret_key)
        self.minio_bucket = os.getenv("MINIO_BUCKET", self.minio_bucket)
        self.minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        self.sync_interval_seconds = int(
            os.getenv("SYNC_INTERVAL_SECONDS", str(self.sync_interval_seconds))
        )

    def is_minio_configured(self) -> bool:
        """Check if MinIO is properly configured."""
        return bool(
            self.minio_endpoint and self.minio_access_key and self.minio_secret_key
        )

    @staticmethod
    def get_settings() -> "Settings":
        """Factory method to create and return a Settings instance."""
        return Settings()
