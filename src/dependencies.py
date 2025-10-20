from initializers import init_logger
from settings import Settings
from logging import Logger

settings: Settings = Settings.get_settings()
logger: Logger = init_logger(settings)
