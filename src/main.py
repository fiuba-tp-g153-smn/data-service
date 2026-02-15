"""Main entrypoint for the data-service application."""

import asyncio
from contextlib import asynccontextmanager
from typing import Optional

import uvloop
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from controller import general
from dependencies import logger, redis_client, settings
from routes import radar, satellite, sync
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


async def configure_strategies(
    client_redis: RedisClient,
) -> tuple[SatelliteSyncStrategy, RadarSyncStrategy, Optional[S3Client]]:
    """Configure and return sync strategies based on settings."""
    s3_client = None

    if settings.sync_mode == "full":
        # Background sync mode (default)
        sat_strategy = SatelliteFullSyncStrategy(client_redis)
        radar_strategy = RadarFullSyncStrategy(client_redis)

        sync_service.set_redis_client(client_redis)
        await sync_service.start(logger)
    else:
        # On-demand mode: lazy fetch + cache
        logger.info("Starting in on-demand sync mode")
        if settings.is_s3_configured():
            s3_client = S3Client(
                endpoint=settings.s3_tiles_data_endpoint,
                access_key=settings.s3_tiles_data_access_key,
                secret_key=settings.s3_tiles_data_secret_key,
                bucket=settings.s3_tiles_data_bucket_name,
                secure=settings.s3_tiles_data_secure,
            )
            await s3_client.connect()

        sat_strategy = SatelliteOnDemandStrategy(
            client_redis,
            s3_client,
            settings.tile_ttl,
            settings.tileset_listing_ttl,
        )
        radar_strategy = RadarOnDemandStrategy(
            client_redis,
            s3_client,
            settings.tile_ttl,
            settings.tileset_listing_ttl,
        )

    return sat_strategy, radar_strategy, s3_client


async def shutdown_services():
    """Stop background services if sync mode is full."""
    if settings.sync_mode == "full":
        await sync_service.stop(logger)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Manage application lifecycle events."""
    # Startup
    logger.info("Starting data-service...")
    await redis_client.connect()

    sat_strategy, radar_strategy, s3_client = await configure_strategies(redis_client)

    satellite_service.set_strategy(sat_strategy)
    radar_service.set_strategy(radar_strategy)

    yield

    # Shutdown
    logger.info("Shutting down data-service...")
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
app.include_router(radar.router)  # Radar routes (most specific)
app.include_router(satellite.router)  # Satellite routes
app.include_router(sync.router)  # Sync observability
