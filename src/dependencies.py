"""Dependency injection module."""

from logging import Logger

from fastapi import Request

from clients.redis_client import RedisClient
from initializers import init_logger
from services.basemap_service import BasemapService
from settings import Settings

settings: Settings = Settings.get_settings()
logger: Logger = init_logger(settings)
redis_client: RedisClient = RedisClient(settings.redis_url)


def get_redis_client() -> RedisClient:
    """FastAPI dependency provider for RedisClient."""
    return redis_client


def get_basemap_service(request: Request) -> BasemapService:
    """FastAPI dependency provider for BasemapService (set on app.state in lifespan)."""
    return request.app.state.basemap_service
