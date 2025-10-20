import os


class Settings:
    """
    Application settings management using Pydantic.
    Handles configuration loading for the application from environment variables.
    """

    log_level: str = ""
    app_env: str = ""
    def __init__(self):
        # Load from environment variables
        self._load_from_env()

    def _load_from_env(self) -> None:
        """Load all configuration values directly from environment variables."""
        self.log_level = os.getenv("LOG_LEVEL", self.log_level)
        self.app_env = os.getenv("APP_ENV", self.app_env)

    @staticmethod
    def get_settings() -> "Settings":
        """Factory method to create and return a Settings instance."""
        return Settings()
