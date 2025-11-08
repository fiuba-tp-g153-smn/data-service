from fastapi import FastAPI

from controller import general
from routes import weather

app: FastAPI = FastAPI(
    title="data-service",
    description="Servicio que maneja la gestión de datos",
    contact={
        "name": "FIUBA TPF Team N°153 Altamirano, Diem, Gismondi, Valeriani",
    },
)

app.include_router(general.router)
app.include_router(weather.router)
