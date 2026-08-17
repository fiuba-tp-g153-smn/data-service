"""Dependency injection module."""

from logging import Logger
from typing import Optional

from clients.basemap_state_store import BasemapStateStore
from clients.metrics_store import MetricsStore
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
redis_client: RedisClient = RedisClient(
    settings.redis_url,
    max_connections=settings.redis_max_connections,
    socket_timeout_seconds=settings.redis_socket_timeout_seconds,
    socket_connect_timeout_seconds=settings.redis_socket_connect_timeout_seconds,
    health_check_interval_seconds=settings.redis_health_check_interval_seconds,
)
metrics_store: MetricsStore = MetricsStore(settings.metrics_db_path)
basemap_service: BasemapService = BasemapService()
# Populated in the lifespan via `set_weather_stations_keystore`. Routes hold a
# Depends() reference, so we can't construct the keystore until the SQLite file
# is opened; the lifespan stamps it here once that's done.
_weather_stations_keystore: Optional[WeatherStationsKeystore] = None
# Populated in the lifespan via `set_basemap_state_store` only when the basemap
# scraper runs. Stays None otherwise; the metrics route degrades to an empty
# provider list rather than failing.
_basemap_state_store: Optional[BasemapStateStore] = None


def get_settings() -> Settings:
    """FastAPI dependency provider for Settings."""
    return settings


def get_redis_client() -> RedisClient:
    """FastAPI dependency provider for RedisClient."""
    return redis_client


def get_metrics_store() -> MetricsStore:
    """FastAPI dependency provider for MetricsStore."""
    return metrics_store


def get_basemap_service() -> BasemapService:
    """FastAPI dependency provider for BasemapService."""
    return basemap_service


def set_basemap_state_store(state_store: BasemapStateStore) -> None:
    """Lifespan hook: register the live basemap state store for metrics reads."""
    global _basemap_state_store  # pylint: disable=global-statement
    _basemap_state_store = state_store


def get_basemap_state_store() -> Optional[BasemapStateStore]:
    """FastAPI dependency provider for the basemap state store (None if scraper off)."""
    return _basemap_state_store


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
