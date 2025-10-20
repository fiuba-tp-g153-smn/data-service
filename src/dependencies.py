from logging import Logger

from initializers import init_logger
from settings import Settings

settings: Settings = Settings.get_settings()
logger: Logger = init_logger(settings)
