import asyncio
from contextlib import asynccontextmanager

import uvloop
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from controller import general
from dependencies import logger, redis_client
from routes import radar, satellite, sync
from services.sync_service import sync_service
from services.satellite_service import satellite_service
from services.radar_service import radar_service
from services.radar_sync_service import radar_sync_service

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle events."""
    # Startup
    logger.info("Starting data-service...")
    await redis_client.connect()

    # Wire Redis into services
    satellite_service.set_redis_client(redis_client)
    radar_service.set_redis_client(redis_client)
    sync_service.set_redis_client(redis_client)

    await sync_service.start(logger)
    await radar_sync_service.start(logger)

    yield

    # Shutdown
    logger.info("Shutting down data-service...")
    await radar_sync_service.stop(logger)
    await sync_service.stop(logger)
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
