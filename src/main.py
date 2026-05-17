"""Main entrypoint for the data-service application."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

import uvloop
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from clients.basemap_state_store import BasemapStateStore
from clients.http_tile_client import HttpTileClient
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from clients.smn_api_client import SmnApiClient
from clients.smn_registry_client import SmnRegistryClient
from clients.weather_stations_keystore import WeatherStationsKeystore
from controller import general
from dependencies import basemap_service, logger, redis_client, settings
from gdal_config import configure_gdal_vsi_s3
from routes import basemap, ecmwf_mslp, ecmwf_tp, radar, satellite, sync
from services.basemap_config import BoundingBox, load_providers
from services.basemap_scraper_service import BasemapScraperService
from services.basemap_tile_reader import BasemapTileReader
from services.ecmwf_mslp_service import ecmwf_mslp_service
from services.ecmwf_mslp_sync_strategy import (
    EcmwfMslpFullSyncStrategy,
    EcmwfMslpOnDemandStrategy,
    EcmwfMslpSyncStrategy,
)
from services.ecmwf_tp_service import ecmwf_tp_service
from services.ecmwf_tp_sync_strategy import (
    EcmwfTpFullSyncStrategy,
    EcmwfTpOnDemandStrategy,
    EcmwfTpSyncStrategy,
)
from services.point_value_service import point_value_service
from services.point_value_strategy import S3CogPointValueStrategy
from services.radar_service import radar_service
from services.radar_sync_strategy import (
    RadarFullSyncStrategy,
    RadarOnDemandStrategy,
    RadarSyncStrategy,
)
from services.satellite_service import satellite_service
from services.satellite_sync_strategy import (
    SatelliteFullSyncStrategy,
    SatelliteOnDemandStrategy,
    SatelliteSyncStrategy,
)
from services.sync_service import sync_service
from services.weather_stations_scraper_service import WeatherStationsScraperService

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@dataclass(slots=True)
class WeatherStationsRuntime:
    """Lifecycle holder for weather-stations resources owned by the app lifespan."""

    keystore: WeatherStationsKeystore
    # All three are absent when sync_mode == "disabled" (keystore stays for
    # read-side auth gating, but nothing scrapes).
    s3_client: Optional[S3Client] = None
    smn_client: Optional[SmnApiClient] = None
    registry_client: Optional[SmnRegistryClient] = None
    scraper: Optional[WeatherStationsScraperService] = None


@dataclass(slots=True)
class BasemapRuntime:
    """Lifecycle holder for basemap-scoped resources owned by the app lifespan."""

    reader_http_client: HttpTileClient
    reader: BasemapTileReader
    # S3 is absent in relay_only mode.
    s3_client: Optional[S3Client] = None
    # Scraper-only resources. Absent in relay_only mode.
    scraper_http_client: Optional[HttpTileClient] = None
    state_store: Optional[BasemapStateStore] = None
    scraper: Optional[BasemapScraperService] = None


async def configure_strategies(
    client_redis: RedisClient,
) -> tuple[
    SatelliteSyncStrategy,
    RadarSyncStrategy,
    EcmwfTpSyncStrategy,
    EcmwfMslpSyncStrategy,
    S3CogPointValueStrategy,
    Optional[S3Client],
]:
    """Configure and return sync strategies based on settings."""
    s3_client = None
    sat_strategy: SatelliteSyncStrategy
    radar_strategy: RadarSyncStrategy
    ecmwf_tp_strategy: EcmwfTpSyncStrategy
    ecmwf_mslp_strategy: EcmwfMslpSyncStrategy

    if settings.is_s3_configured():
        s3_client = S3Client(
            endpoint=settings.s3_tiles_data_endpoint,
            access_key=settings.s3_tiles_data_access_key,
            secret_key=settings.s3_tiles_data_secret_key,
            bucket=settings.s3_tiles_data_bucket_name,
            secure=settings.s3_tiles_data_secure,
            max_concurrent_downloads=settings.s3_max_concurrent_downloads,
        )
        await s3_client.connect()

    point_value_strategy = S3CogPointValueStrategy(s3_client)

    if settings.sync_mode == "full":
        # Background sync mode (default)
        sat_strategy = SatelliteFullSyncStrategy(client_redis)
        radar_strategy = RadarFullSyncStrategy(client_redis)
        ecmwf_tp_strategy = EcmwfTpFullSyncStrategy(client_redis)
        ecmwf_mslp_strategy = EcmwfMslpFullSyncStrategy(client_redis)

        sync_service.set_redis_client(client_redis)
        await sync_service.start(logger)
    else:
        # On-demand mode: lazy fetch + cache
        logger.info("Starting in on-demand sync mode")

        sat_strategy = SatelliteOnDemandStrategy(
            client_redis,
            s3_client,
            settings.tile_ttl,
            settings.tileset_listing_ttl,
        )
        radar_strategy = RadarOnDemandStrategy(
            client_redis,
            s3_client,
            settings.radar_tile_ttl,
            settings.tileset_listing_ttl,
        )
        ecmwf_tp_strategy = EcmwfTpOnDemandStrategy(
            client_redis,
            s3_client,
            settings.ecmwf_tile_ttl,
            settings.tileset_listing_ttl,
        )
        ecmwf_mslp_strategy = EcmwfMslpOnDemandStrategy(
            client_redis,
            s3_client,
            settings.ecmwf_mslp_geojson_ttl,
            settings.tileset_listing_ttl,
        )

    return (
        sat_strategy,
        radar_strategy,
        ecmwf_tp_strategy,
        ecmwf_mslp_strategy,
        point_value_strategy,
        s3_client,
    )


async def configure_basemap(
    client_redis: RedisClient,
) -> Optional[BasemapRuntime]:
    """Bring up the basemap subsystem using the active `basemap_sync_mode`.

    Modes (set via `settings.json::basemap_sync_mode` or `BASEMAP_SYNC_MODE`):
      * ``full``       — scraper on (writes Redis + S3); reader tries
                         upstream first, then falls back to Redis, then S3.
      * ``on_demand``  — scraper on but writes only S3; reader tries
                         upstream first, then Redis (lazily populated by
                         the reader on hits), then S3.
      * ``no_cache``   — scraper on, S3-only. Reader skips Redis tier
                         entirely (upstream → S3).
      * ``relay_only`` — scraper off, Redis off, S3 off. Reader is a pure
                         provider proxy.

    `relay_only` is the only mode that skips S3 reads, so an S3 backend
    outage doesn't take the service down under `relay_only`. For every
    other mode, S3 must be configured — a cold backup without S3 storage
    defeats the point of having a backup.

    When enabled, populates the module-level `basemap_service` singleton via
    `configure()` and returns its backing runtime for lifespan-scoped shutdown.
    When disabled, the singleton keeps its empty default state.
    """
    providers = load_providers(settings.basemap_providers)

    if not providers:
        logger.info("Basemap disabled: no providers enabled in settings.json")
        return None

    mode = settings.basemap_sync_mode
    run_scraper = mode in ("full", "on_demand", "no_cache")
    scraper_writes_redis = mode == "full"
    redis_cache_enabled = mode in ("full", "on_demand")
    s3_cache_enabled = mode in ("full", "on_demand", "no_cache")

    if s3_cache_enabled and not settings.is_s3_configured():
        logger.error(
            "Basemap refused to start: S3 is not configured but basemap "
            "mode=%s requires S3 storage. Configure S3 credentials, switch "
            "to basemap_sync_mode=relay_only, or disable basemap_providers "
            "in settings.json.",
            mode,
        )
        return None

    logger.info(
        "Basemap mode=%s (scraper=%s, scraper_redis=%s, "
        "reader_redis=%s, reader_s3=%s)",
        mode,
        "on" if run_scraper else "off",
        "on" if scraper_writes_redis else "off",
        "on" if redis_cache_enabled else "off",
        "on" if s3_cache_enabled else "off",
    )

    basemap_s3: Optional[S3Client] = None
    if s3_cache_enabled:
        basemap_s3 = S3Client(
            endpoint=settings.s3_tiles_data_endpoint,
            access_key=settings.s3_tiles_data_access_key,
            secret_key=settings.s3_tiles_data_secret_key,
            bucket=settings.s3_basemap_bucket_name,
            secure=settings.s3_tiles_data_secure,
            max_concurrent_downloads=settings.s3_max_concurrent_downloads,
        )
        await basemap_s3.connect()
        # Lifecycle policy application is delegated to the scraper loop so
        # a transient S3 outage at boot can self-heal on the next sweep
        # instead of leaving the bucket with the wrong (or no) rule.

    # Separate pool for user-facing reads so tile requests can't queue behind
    # the scraper's retry loop. Tight budget: short timeout, minimal retries.
    reader_http_client = HttpTileClient(
        max_concurrent=settings.basemap_reader_http_concurrent,
        delay_ms=0,
        timeout_seconds=settings.basemap_reader_http_timeout_seconds,
        max_retries=settings.basemap_reader_http_max_retries,
    )
    await reader_http_client.connect()

    reader = BasemapTileReader(
        redis_client=client_redis,
        s3_client=basemap_s3,
        http_client=reader_http_client,
        providers=providers,
        tile_ttl=settings.basemap_tile_ttl,
        cache_concurrent=settings.basemap_cache_concurrent,
        online_fallback=settings.basemap_online_fallback_enabled,
        request_deadline_seconds=settings.basemap_request_deadline_seconds,
        redis_cache_enabled=redis_cache_enabled,
        s3_cache_enabled=s3_cache_enabled,
    )

    scraper_http_client: Optional[HttpTileClient] = None
    state_store: Optional[BasemapStateStore] = None
    scraper: Optional[BasemapScraperService] = None
    if run_scraper:
        scraper_http_client = HttpTileClient(
            max_concurrent=settings.basemap_scrape_concurrent,
            delay_ms=settings.basemap_scrape_delay_ms,
            timeout_seconds=settings.basemap_http_timeout_seconds,
            max_retries=settings.basemap_http_max_retries,
            per_host_concurrent=settings.basemap_scrape_per_host_concurrent,
        )
        await scraper_http_client.connect()

        bbox = BoundingBox(
            lat_min=settings.basemap_bbox_lat_min,
            lat_max=settings.basemap_bbox_lat_max,
            lon_min=settings.basemap_bbox_lon_min,
            lon_max=settings.basemap_bbox_lon_max,
        )

        state_store = BasemapStateStore(settings.basemap_scrape_state_db_path)
        await state_store.connect()

        # run_scraper implies s3_cache_enabled by construction, so basemap_s3
        # is guaranteed non-None here. Assert for the benefit of mypy.
        assert basemap_s3 is not None
        scraper = BasemapScraperService(
            settings=settings,
            s3_client=basemap_s3,
            redis_client=client_redis,
            http_client=scraper_http_client,
            state_store=state_store,
            providers=providers,
            bbox=bbox,
            tile_ttl=settings.basemap_tile_ttl,
            s3_object_ttl_days=settings.basemap_s3_object_ttl_days,
            redis_writes_enabled=scraper_writes_redis,
            parallelism_mode=settings.basemap_scrape_parallelism_mode,
        )
        await scraper.start(logger)

    basemap_service.configure(
        reader=reader,
        providers=providers,
        online_fallback=settings.basemap_online_fallback_enabled,
        s3_client=basemap_s3,
        redis_client=client_redis,
        http_client=reader_http_client,
        availability_ttl=settings.basemap_provider_availability_ttl,
    )
    return BasemapRuntime(
        s3_client=basemap_s3,
        reader_http_client=reader_http_client,
        reader=reader,
        scraper_http_client=scraper_http_client,
        state_store=state_store,
        scraper=scraper,
    )


async def configure_weather_stations() -> WeatherStationsRuntime:
    """Bring up the weather-stations subsystem.

    The keystore is always built (it gates the read endpoints' API-key auth
    even when no scraper runs). When `weather_stations_sync_mode == "full"`
    the scraper + its S3/SMN clients are also built and started. Subsystem
    is S3-only by design — no Redis.
    """
    keystore = WeatherStationsKeystore(settings.weather_stations_keystore_db_path)
    await keystore.connect()

    if settings.weather_stations_sync_mode == "disabled":
        logger.info("Weather stations scraper disabled (sync_mode=disabled)")
        return WeatherStationsRuntime(keystore=keystore)

    if not settings.is_s3_configured():
        logger.error(
            "Weather stations refused to start: S3 is not configured but "
            "weather_stations_sync_mode=full requires S3. Configure S3 "
            "credentials or set WEATHER_STATIONS_SYNC_MODE=disabled."
        )
        return WeatherStationsRuntime(keystore=keystore)

    weather_s3 = S3Client(
        endpoint=settings.s3_tiles_data_endpoint,
        access_key=settings.s3_tiles_data_access_key,
        secret_key=settings.s3_tiles_data_secret_key,
        bucket=settings.s3_weather_stations_bucket_name,
        secure=settings.s3_tiles_data_secure,
        max_concurrent_downloads=settings.s3_max_concurrent_downloads,
    )
    await weather_s3.connect()

    smn_client = SmnApiClient(
        base_url=settings.smn_api_base_url,
        username=settings.smn_api_username,
        password=settings.smn_api_password,
        timeout_seconds=settings.weather_stations_http_timeout_seconds,
        max_retries=settings.weather_stations_http_max_retries,
        token_cache_ttl_seconds=settings.weather_stations_token_cache_ttl_seconds,
    )
    registry_client = SmnRegistryClient(
        url=settings.smn_stations_registry_url,
        timeout_seconds=settings.weather_stations_http_timeout_seconds,
        max_retries=settings.weather_stations_http_max_retries,
    )

    scraper = WeatherStationsScraperService(
        settings=settings,
        s3_client=weather_s3,
        smn_client=smn_client,
        registry_client=registry_client,
    )
    await scraper.start(logger)

    return WeatherStationsRuntime(
        keystore=keystore,
        s3_client=weather_s3,
        smn_client=smn_client,
        registry_client=registry_client,
        scraper=scraper,
    )


async def shutdown_weather_stations(runtime: WeatherStationsRuntime) -> None:
    """Tear down weather-stations resources in reverse startup order."""
    if runtime.scraper is not None:
        await runtime.scraper.stop(logger)
    if runtime.smn_client is not None:
        await runtime.smn_client.close()
    if runtime.registry_client is not None:
        await runtime.registry_client.close()
    if runtime.s3_client is not None:
        await runtime.s3_client.close()
    await runtime.keystore.close()


async def shutdown_basemap(runtime: Optional[BasemapRuntime]) -> None:
    """Tear down basemap resources in reverse startup order."""
    if not runtime:
        return
    if runtime.scraper is not None:
        await runtime.scraper.stop(logger)
    if runtime.state_store is not None:
        await runtime.state_store.close()
    await runtime.reader.close()
    await runtime.reader_http_client.close()
    if runtime.scraper_http_client is not None:
        await runtime.scraper_http_client.close()
    if runtime.s3_client is not None:
        await runtime.s3_client.close()


async def shutdown_services():
    """Stop background services if sync mode is full."""
    if settings.sync_mode == "full":
        await sync_service.stop(logger)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Manage application lifecycle events."""
    logger.info("Starting data-service...")
    configure_gdal_vsi_s3()
    await redis_client.connect()

    (
        sat_strategy,
        radar_strategy,
        ecmwf_tp_strategy,
        ecmwf_mslp_strategy,
        point_value_strategy,
        s3_client,
    ) = await configure_strategies(redis_client)

    satellite_service.set_strategy(sat_strategy)
    radar_service.set_strategy(radar_strategy)
    ecmwf_tp_service.set_strategy(ecmwf_tp_strategy)
    ecmwf_mslp_service.set_strategy(ecmwf_mslp_strategy)
    point_value_service.set_strategy(point_value_strategy)

    basemap_runtime = await configure_basemap(redis_client)
    weather_stations_runtime = await configure_weather_stations()

    yield

    # Shutdown
    logger.info("Shutting down data-service...")
    await shutdown_weather_stations(weather_stations_runtime)
    await shutdown_basemap(basemap_runtime)
    await shutdown_services()

    if s3_client:
        await s3_client.close()
    await redis_client.close()


app: FastAPI = FastAPI(
    title="data-service",
    description="Servicio que maneja la gestión de datos",
    contact={
        "name": "FIUBA TPF Team N°153 Altamirano, Diem, Gismondi, Valeriani",
    },
    lifespan=lifespan,
)

# Add CORS middleware for tile serving
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(general.router)
app.include_router(basemap.router)  # Base map tile proxy
app.include_router(radar.router)  # Radar routes (most specific)
app.include_router(ecmwf_tp.router)  # ECMWF total precipitation routes
app.include_router(ecmwf_mslp.router)  # ECMWF mean sea level pressure routes
app.include_router(satellite.router)  # Satellite routes
app.include_router(sync.router)  # Sync observability
