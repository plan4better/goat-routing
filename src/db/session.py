from sqlalchemy.engine import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    AsyncConnection,
    create_async_engine,
)


from src.core.config import settings

async_engine = create_async_engine(settings.ASYNC_SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
sync_engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, future=False)