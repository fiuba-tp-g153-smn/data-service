from fastapi import FastAPI

from controller import general

app: FastAPI = FastAPI(
    title="users-service",
    description="Servicio que maneja la gestión de usuarios",
    contact={
        "name": "FIUBA TPF Team N°153 Altamirano, Diem, Gismondi, Valeriani",
    },
)

app.include_router(general.router)
