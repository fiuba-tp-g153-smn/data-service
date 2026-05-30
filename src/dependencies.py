"""Dependency injection module."""

from logging import Logger
from typing import Optional

from clients.redis_client import RedisClient
from clients.weather_stations_keystore import WeatherStationsKeystore
from initializers import init_logger
from services.basemap_service import BasemapService
from services.weather_stations_service import (
    WeatherStationsService,
    weather_stations_service,
)
from settings import Settings

settings: Settings = Settings.get_settings()
logger: Logger = init_logger(settings)
redis_client: RedisClient = RedisClient(settings.redis_url)
basemap_service: BasemapService = BasemapService()
# Populated in the lifespan via `set_weather_stations_keystore`. Routes hold a
# Depends() reference, so we can't construct the keystore until the SQLite file
# is opened; the lifespan stamps it here once that's done.
_weather_stations_keystore: Optional[WeatherStationsKeystore] = None


def get_redis_client() -> RedisClient:
    """FastAPI dependency provider for RedisClient."""
    return redis_client


def get_basemap_service() -> BasemapService:
    """FastAPI dependency provider for BasemapService."""
    return basemap_service


def get_weather_stations_service() -> WeatherStationsService:
    """FastAPI dependency provider for WeatherStationsService."""
    return weather_stations_service


def set_weather_stations_keystore(keystore: WeatherStationsKeystore) -> None:
    """Lifespan hook: register the live keystore for the auth dependency."""
    global _weather_stations_keystore  # pylint: disable=global-statement
    _weather_stations_keystore = keystore


def get_weather_stations_keystore() -> WeatherStationsKeystore:
    """FastAPI dependency provider for WeatherStationsKeystore."""
    if _weather_stations_keystore is None:
        raise RuntimeError(
            "Weather stations keystore is not configured "
            "(lifespan did not register it)."
        )
    return _weather_stations_keystore
