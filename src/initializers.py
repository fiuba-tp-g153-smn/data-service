"""Application initializers."""

import logging
from logging import Logger

from newrelic.agent import NewRelicContextFormatter

from settings import Settings

DEFAULT_LOG_LEVEL: str = "INFO"
APP_ENV_DEVELOPMENT: str = "development"


def init_logger(settings: Settings) -> Logger:
    """Initialize the application logger."""
    handler = logging.StreamHandler()
    if settings.app_env == APP_ENV_DEVELOPMENT:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    else:
        formatter = NewRelicContextFormatter()
    handler.setFormatter(formatter)
    log_level = Settings.get_settings().log_level or DEFAULT_LOG_LEVEL

    # Configure the root logger so all module-level loggers (services.*, clients.*)
    # propagate through the same handler and level.
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    # Silence httpx's per-request INFO lines; WARNING+ still surfaces.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    # Named logger returned for direct use in main/dependencies.
    # No handler attached — inherits from root to avoid duplicate output.
    logger = logging.getLogger("data-service")
    logger.setLevel(log_level)
    return logger
