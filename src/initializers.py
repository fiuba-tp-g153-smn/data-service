import logging
from logging import Logger
from newrelic.agent import NewRelicContextFormatter

from settings import Settings

DEFAULT_LOG_LEVEL: str = "INFO"
APP_ENV_DEVELOPMENT: str = "development"

def init_logger(settings: Settings) -> Logger:
    handler = logging.StreamHandler()
    if settings.app_env == APP_ENV_DEVELOPMENT:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    else:
        formatter = NewRelicContextFormatter()
    handler.setFormatter(formatter)
    log_level = Settings.get_settings().log_level or DEFAULT_LOG_LEVEL

    logger = logging.getLogger("users-service")
    logger.setLevel(log_level)
    logger.handlers.clear()
    logger.addHandler(handler)

    return logger
