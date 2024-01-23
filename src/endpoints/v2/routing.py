import json

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse
from redis import Redis

from src.core.config import settings
from src.core.worker import run_isochrone
from src.schemas.isochrone import IIsochroneActiveMobility
from src.schemas.isochrone import request_examples as active_mobility_request_examples
from src.schemas.status import ProcessingStatus

router = APIRouter()
redis = Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
)


@router.post(
    "/isochrone",
    summary="Compute isochrones for active mobility",
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

    # Get processing status of isochrone request
    processing_status = redis.get(str(params.layer_id))
    processing_status = processing_status.decode("utf-8") if processing_status else None

    if processing_status is None:
        # Initiate isochrone computation for request
        redis.set(str(params.layer_id), ProcessingStatus.in_progress.value)
        params = json.loads(params.json()).copy()
        run_isochrone.delay(params)
        return JSONResponse(
            content={
                "result": ProcessingStatus.in_progress.value,
                "message": "Isochrone computation in progress.",
            },
            status_code=202,
        )
    elif processing_status == ProcessingStatus.in_progress.value:
        # Isochrone computation is in progress
        return JSONResponse(
            content={
                "result": processing_status,
                "message": "Isochrone computation in progress.",
            },
            status_code=202,
        )
    elif processing_status == ProcessingStatus.success.value:
        # Isochrone computation was successful
        return JSONResponse(
            content={
                "result": processing_status,
                "message": "Isochrone computed successfully.",
            },
            status_code=201,
        )
    elif processing_status == ProcessingStatus.disconnected_origin.value:
        # Isochrone computation failed
        return JSONResponse(
            content={
                "result": processing_status,
                "message": "Starting point(s) are disconnected from the street network.",
            },
            status_code=400,
        )
    else:
        # Isochrone computation failed
        return JSONResponse(
            content={
                "result": processing_status,
                "message": "Failed to compute isochrone.",
            },
            status_code=500,
        )
