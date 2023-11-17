# Standard library imports
import asyncio

# Third party imports
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text

# Local application imports
from src.main import app


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def session_fixture(event_loop):
    session_manager.init(settings.ASYNC_SQLALCHEMY_DATABASE_URI)
    session_manager._engine.update_execution_options(
        schema_translate_map={"customer": schema_customer}
    )
    async with session_manager.connect() as connection:
        await connection.execute(
            text(f"""CREATE SCHEMA IF NOT EXISTS {schema_customer}""")
        )
        await connection.execute(
            text(f"""CREATE SCHEMA IF NOT EXISTS {schema_user_data}""")
        )
        await session_manager.drop_all(connection)
        await session_manager.create_all(connection)
    yield
    async with session_manager.connect() as connection:
        pass
        await connection.execute(
            text(f"""DROP SCHEMA IF EXISTS {schema_customer} CASCADE""")
        )
        await connection.execute(
            text(f"""DROP SCHEMA IF EXISTS {schema_user_data} CASCADE""")
        )
    await session_manager.close()


@pytest_asyncio.fixture(autouse=True)
async def session_override(session_fixture):
    async def get_db_override():
        async with session_manager.session() as session:
            yield session

    app.dependency_overrides[get_db] = get_db_override


@pytest_asyncio.fixture
async def db_session():
    async with session_manager.session() as session:
        yield session
