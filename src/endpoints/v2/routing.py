# Standard Libraries

# Third-party Libraries
from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse

# Project-specific Modules
from src.crud.crud_isochrone import CRUDIsochrone, FetchRoutingNetwork
from src.schemas.isochrone import IIsochroneActiveMobility
from src.schemas.isochrone import request_examples as active_mobility_request_examples

router = APIRouter()
routing_network = None


def get_routing_network():
    """Manages a shared object for our in-memory routing network."""

    # Initialize routing network on startup
    global routing_network
    routing_network = (
        FetchRoutingNetwork().fetch() if routing_network is None else routing_network
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
    routing_network: dict = Depends(get_routing_network),
    params: IIsochroneActiveMobility = Body(
        ...,
        examples=active_mobility_request_examples["isochrone_active_mobility"],
        description="The isochrone parameters.",
    ),
):
    """Compute isochrones for active mobility."""

    CRUDIsochrone().run(routing_network, params)

    return {"result": "success", "message": "Isochrone computed successfully."}
