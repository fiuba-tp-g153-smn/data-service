"""Main entrypoint for the data-service application."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

import uvloop
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from clients.http_tile_client import HttpTileClient
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from controller import general
from dependencies import basemap_service, logger, redis_client, settings
from gdal_config import configure_gdal_vsi_s3
from routes import basemap, ecmwf, radar, satellite, sync
from services.basemap_config import BoundingBox, load_providers
from services.basemap_scraper_service import BasemapScraperService
from services.basemap_tile_reader import BasemapTileReader
from services.ecmwf_service import ecmwf_service
from services.ecmwf_sync_strategy import EcmwfFullSyncStrategy, EcmwfOnDemandStrategy
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

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@dataclass(slots=True)
class BasemapRuntime:
    """Lifecycle holder for basemap-scoped resources owned by the app lifespan."""

    s3_client: S3Client
    http_client: HttpTileClient
    reader: BasemapTileReader
    scraper: BasemapScraperService


async def configure_strategies(
    client_redis: RedisClient,
) -> tuple[
    SatelliteSyncStrategy,
    RadarSyncStrategy,
    S3CogPointValueStrategy,
    Optional[S3Client],
]:
    """Configure and return sync strategies based on settings."""
    s3_client = None
    sat_strategy: SatelliteSyncStrategy
    radar_strategy: RadarSyncStrategy

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

    # ECMWF precipitation (independently configurable sync mode)
    if settings.sync_mode == "full":
        ecmwf_strategy = EcmwfFullSyncStrategy(client_redis)
        ecmwf_service.set_s3_client(s3_client)
        ecmwf_service.set_redis_client(client_redis)
        await ecmwf_service.start_sync(logger)
    else:
        logger.info("Starting ECMWF in on-demand sync mode")
        ecmwf_strategy = EcmwfOnDemandStrategy(
            client_redis,
            s3_client,
            settings.ecmwf_tile_ttl,
            settings.tileset_listing_ttl,
        )
    ecmwf_service.set_strategy(ecmwf_strategy)

    return sat_strategy, radar_strategy, point_value_strategy, s3_client


async def configure_basemap(
    client_redis: RedisClient,
) -> Optional[BasemapRuntime]:
    """Bring up the basemap subsystem (reader + mandatory full-sync scraper).

    Basemap ignores `settings.sync_mode`: a backup of provider tiles requires
    the scraper to always run when enabled. If S3 is not configured, basemap
    is refused entirely rather than silently degrading to pure provider-proxy
    mode — which would defeat the backup purpose.

    When enabled, populates the module-level `basemap_service` singleton via
    `configure()` and returns its backing runtime for lifespan-scoped shutdown.
    When disabled, the singleton keeps its empty default state.
    """
    providers = load_providers(settings.basemap_providers)

    if not providers:
        logger.info("Basemap disabled: no providers enabled in settings.json")
        return None

    if not settings.is_s3_configured():
        logger.error(
            "Basemap refused to start: S3 is not configured but basemap "
            "requires full-sync storage. Configure S3 credentials or disable "
            "basemap_providers in settings.json."
        )
        return None

    basemap_s3 = S3Client(
        endpoint=settings.s3_tiles_data_endpoint,
        access_key=settings.s3_tiles_data_access_key,
        secret_key=settings.s3_tiles_data_secret_key,
        bucket=settings.s3_basemap_bucket_name,
        secure=settings.s3_tiles_data_secure,
        max_concurrent_downloads=settings.s3_max_concurrent_downloads,
    )
    await basemap_s3.connect()
    await basemap_s3.ensure_lifecycle_expiration(settings.basemap_s3_object_ttl_days)

    http_client = HttpTileClient(
        max_concurrent=settings.basemap_scrape_concurrent,
        delay_ms=settings.basemap_scrape_delay_ms,
        timeout_seconds=settings.basemap_http_timeout_seconds,
        max_retries=settings.basemap_http_max_retries,
    )
    await http_client.connect()

    reader = BasemapTileReader(
        redis_client=client_redis,
        s3_client=basemap_s3,
        http_client=http_client,
        providers=providers,
        tile_ttl=settings.basemap_tile_ttl,
        cache_concurrent=settings.basemap_cache_concurrent,
        online_fallback=settings.basemap_online_fallback_enabled,
    )

    bbox = BoundingBox(
        lat_min=settings.basemap_bbox_lat_min,
        lat_max=settings.basemap_bbox_lat_max,
        lon_min=settings.basemap_bbox_lon_min,
        lon_max=settings.basemap_bbox_lon_max,
    )

    scraper = BasemapScraperService(
        settings=settings,
        s3_client=basemap_s3,
        redis_client=client_redis,
        http_client=http_client,
        providers=providers,
        bbox=bbox,
        tile_ttl=settings.basemap_tile_ttl,
    )
    await scraper.start(logger)

    basemap_service.configure(reader=reader, providers=providers)
    return BasemapRuntime(
        s3_client=basemap_s3,
        http_client=http_client,
        reader=reader,
        scraper=scraper,
    )


async def shutdown_basemap(runtime: Optional[BasemapRuntime]) -> None:
    """Tear down basemap resources in reverse startup order."""
    if not runtime:
        return
    await runtime.scraper.stop(logger)
    await runtime.reader.close()
    await runtime.http_client.close()
    await runtime.s3_client.close()


async def shutdown_services():
    """Stop background services if sync mode is full."""
    if settings.sync_mode == "full":
        await sync_service.stop(logger)
    if settings.sync_mode == "full":
        await ecmwf_service.stop_sync(logger)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Manage application lifecycle events."""
    logger.info("Starting data-service...")
    configure_gdal_vsi_s3()
    await redis_client.connect()

    sat_strategy, radar_strategy, point_value_strategy, s3_client = (
        await configure_strategies(redis_client)
    )

    satellite_service.set_strategy(sat_strategy)
    radar_service.set_strategy(radar_strategy)
    point_value_service.set_strategy(point_value_strategy)

    basemap_runtime = await configure_basemap(redis_client)

    yield

    # Shutdown
    logger.info("Shutting down data-service...")
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
app.include_router(ecmwf.router)  # ECMWF precipitation routes
app.include_router(satellite.router)  # Satellite routes
app.include_router(sync.router)  # Sync observability
