from fastapi import FastAPI

from controller import endpoints as endpoints_module

app = FastAPI(title="BD2 Proyecto1 API")

app.include_router(endpoints_module.router, prefix="/api")
