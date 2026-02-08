from logging import Logger

from initializers import init_logger
from settings import Settings
from clients.redis_client import RedisClient

settings: Settings = Settings.get_settings()
logger: Logger = init_logger(settings)
redis_client: RedisClient = RedisClient(settings.redis_url)


def get_redis_client() -> RedisClient:
    """FastAPI dependency provider for RedisClient."""
    return redis_client
