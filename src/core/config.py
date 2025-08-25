from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseSettings, PostgresDsn, validator


class AsyncPostgresDsn(PostgresDsn):
    allowed_schemes = {"postgres+asyncpg", "postgresql+asyncpg"}


# For old versions of SQLAlchemy (< 1.4)
class SyncPostgresDsn(PostgresDsn):
    allowed_schemes = {"postgresql", "postgresql+psycopg2", "postgresql+pg8000"}


class Settings(BaseSettings):
    # Monitoring
    SENTRY_DSN: Optional[str] = None
    ENVIRONMENT: Optional[str] = "dev"

    CUSTOMER_SCHEMA: str = "customer"
    USER_DATA_SCHEMA: str = "user_data"

    API_V2_STR: str = "/api/v2"
    PROJECT_NAME: Optional[str] = "GOAT Routing API"
    CACHE_DIR: str = "/app/src/cache"

    NETWORK_REGION_TABLE = "basic.geofence_active_mobility"
    HEATMAP_MATRIX_DATE_SUFFIX = "20250210"

    CATCHMENT_AREA_CAR_BUFFER_DEFAULT_SPEED = 80  # km/h
    CATCHMENT_AREA_HOLE_THRESHOLD_SQM = 200000  # 20 hectares, ~450m x 450m

    BASE_STREET_NETWORK: UUID = UUID("903ecdca-b717-48db-bbce-0219e41439cf")
    DEFAULT_STREET_NETWORK_EDGE_LAYER_PROJECT_ID = (
        36126  # Hardcoded for heatmap matrix preparation
    )
    DEFAULT_STREET_NETWORK_NODE_LAYER_PROJECT_ID = (
        37319  # Hardcoded for heatmap matrix preparation
    )

    DATA_INSERT_BATCH_SIZE = 800

    CELERY_BROKER_URL: Optional[str] = "pyamqp://guest@rabbitmq//"
    REDIS_HOST: Optional[str] = "redis"
    REDIS_PORT: Optional[str] = 6379
    REDIS_DB: Optional[str] = 0

    POSTGRES_SERVER: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_PORT: Optional[str] = "5432"
    POSTGRES_DATABASE_URI: str = None

    MOTIS_HOST: Optional[str] = "motis"
    MOTIS_PORT: Optional[str] = "8080"
    MOTIS_BASE_URL: str = None
    MOTIS_PLAN_ENDPOINT: str = None

    @validator("MOTIS_BASE_URL", pre=True)
    def motis_base_url(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        return f'http://{values.get("MOTIS_HOST")}:{values.get("MOTIS_PORT")}'

    @validator("MOTIS_PLAN_ENDPOINT")
    def motis_plan_endpoint(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        return f'{values.get("MOTIS_BASE_URL")}/api/v4/plan/'

    @validator("POSTGRES_DATABASE_URI", pre=True)
    def postgres_database_uri_(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        return f'postgresql://{values.get("POSTGRES_USER")}:{values.get("POSTGRES_PASSWORD")}@{values.get("POSTGRES_SERVER")}:{values.get("POSTGRES_PORT")}/{values.get("POSTGRES_DB")}'

    ASYNC_SQLALCHEMY_DATABASE_URI: Optional[AsyncPostgresDsn] = None

    @validator("ASYNC_SQLALCHEMY_DATABASE_URI", pre=True)
    def assemble_async_db_connection(
        cls, v: Optional[str], values: Dict[str, Any]
    ) -> Any:
        if isinstance(v, str):
            return v
        return AsyncPostgresDsn.build(
            scheme="postgresql+asyncpg",
            user=values.get("POSTGRES_USER"),
            password=values.get("POSTGRES_PASSWORD"),
            host=values.get("POSTGRES_SERVER"),
            port=values.get("POSTGRES_PORT"),
            path=f"/{values.get('POSTGRES_DB') or ''}",
        )

    SQLALCHEMY_DATABASE_URI: Optional[SyncPostgresDsn] = None

    @validator("SQLALCHEMY_DATABASE_URI", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return SyncPostgresDsn.build(
            scheme="postgresql",
            user=values.get("POSTGRES_USER"),
            password=values.get("POSTGRES_PASSWORD"),
            host=values.get("POSTGRES_SERVER"),
            port=values.get("POSTGRES_PORT"),
            path=f"/{values.get('POSTGRES_DB') or ''}",
        )

    class Config:
        case_sensitive = True


settings = Settings()
