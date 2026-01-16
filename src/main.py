from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from controller import general
from routes import weather, products, satellite, radar
from services.sync_service import sync_service
from dependencies import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle events."""
    # Startup
    logger.info("Starting data-service...")
    await sync_service.start()

    yield

    # Shutdown
    logger.info("Shutting down data-service...")
    await sync_service.stop()


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
app.include_router(weather.router)
app.include_router(radar.router)      # Radar routes (most specific)
app.include_router(satellite.router)  # Satellite routes
app.include_router(products.router)   # General products list (least specific)