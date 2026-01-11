from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from controller import general
from routes import weather, products, radar

app: FastAPI = FastAPI(
    title="data-service",
    description="Servicio que maneja la gestión de datos",
    contact={
        "name": "FIUBA TPF Team N°153 Altamirano, Diem, Gismondi, Valeriani",
    },
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
app.include_router(radar.router)  # Radar routes first (more specific paths)
app.include_router(products.router)