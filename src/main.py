import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

from src.core.config import settings
from src.endpoints.v2.api import router as api_router_v2

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up...")
    logger = logging.getLogger("uvicorn.access")
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    yield
    print("Shutting down...")


app = FastAPI(
    title=settings.PROJECT_NAME,
    redoc_url="/api/redoc",
    openapi_url=f"{settings.API_V2_STR}/openapi.json",
    lifespan=lifespan,
)

@app.get("/api/docs", include_in_schema=False)
async def swagger_ui_html():
    return get_swagger_ui_html(
        swagger_favicon_url="/static/api_favicon.png",
        openapi_url=f"{settings.API_V2_STR}/openapi.json",
        title=settings.PROJECT_NAME,
        swagger_ui_parameters={"persistAuthorization": True},
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router_v2, prefix=settings.API_V2_STR)