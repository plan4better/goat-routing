# Standard Libraries
from uuid import UUID, uuid4

# Third-party Libraries
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse
# Project-specific Modules
from src.schemas.isochrone import IIsochroneActiveMobility, request_examples as active_mobility_request_examples


router = APIRouter()

@router.post(
    "/isochrone",
    summary="Compute isochrones for active mobility",
    response_class=JSONResponse,
    status_code=201,
)
async def compute_active_mobility_isochrone(
    *,
    params: IIsochroneActiveMobility = Body(
        ...,
        examples=active_mobility_request_examples["isochrone_active_mobility"],
        description="The isochrone parameters.",
    ),
):
    """Compute isochrones for active mobility."""
    return {"result": "success", "message": "Isochrone computed successfully."}
