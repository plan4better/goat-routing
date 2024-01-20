# Standard Libraries

# Third-party Libraries

from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

# Project-specific Modules
from src.crud.crud_isochrone import CRUDIsochrone, FetchRoutingNetwork
from src.db.session import async_session
from src.schemas.isochrone import IIsochroneActiveMobility
from src.schemas.isochrone import request_examples as active_mobility_request_examples

router = APIRouter()
routing_network: dict = None


async def get_db_connection():
    """Manages a shared object for an async database connection."""
    async with async_session() as session:
        yield session


async def get_routing_network(
    database_connection: AsyncSession = Depends(get_db_connection),
):
    """Manages a shared object for our in-memory routing network."""

    # Initialize routing network on startup
    global routing_network
    routing_network = (
        await FetchRoutingNetwork(database_connection).fetch()
        if routing_network is None
        else routing_network
    )

    return routing_network


@router.post(
    "/isochrone",
    summary="Compute isochrones for active mobility",
    response_class=JSONResponse,
    status_code=201,
)
async def compute_active_mobility_isochrone(
    *,
    db_connection: AsyncSession = Depends(get_db_connection),
    routing_network: dict = Depends(get_routing_network),
    params: IIsochroneActiveMobility = Body(
        ...,
        examples=active_mobility_request_examples["isochrone_active_mobility"],
        description="The isochrone parameters.",
    ),
):
    """Compute isochrones for active mobility."""

    await CRUDIsochrone(db_connection).run(routing_network, params)

    return {"result": "success", "message": "Isochrone computed successfully."}
