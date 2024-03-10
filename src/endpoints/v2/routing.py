import json

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse
from redis import Redis

from src.core.config import settings
from src.core.worker import run_catchment_area
from src.schemas.catchment_area import ICatchmentAreaActiveMobility
from src.schemas.catchment_area import (
    request_examples as active_mobility_request_examples,
)
from src.schemas.status import ProcessingStatus

router = APIRouter()
redis = Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
)


@router.post(
    "/catchment-area",
    summary="Compute catchment areas for active mobility",
)
async def compute_active_mobility_catchment_area(
    *,
    params: ICatchmentAreaActiveMobility = Body(
        ...,
        examples=active_mobility_request_examples["catchment_area_active_mobility"],
        description="The catchment area parameters.",
    ),
):
    """Compute catchment areas for active mobility."""

    # Get processing status of catchment area request
    processing_status = redis.get(str(params.layer_id))
    processing_status = processing_status.decode("utf-8") if processing_status else None

    if processing_status is None:
        # Initiate catchment area computation for request
        redis.set(str(params.layer_id), ProcessingStatus.in_progress.value)
        params = json.loads(params.json()).copy()
        run_catchment_area.delay(params)
        return JSONResponse(
            content={
                "result": ProcessingStatus.in_progress.value,
                "message": "Catchment area computation in progress.",
            },
            status_code=202,
        )
    elif processing_status == ProcessingStatus.in_progress.value:
        # Catchment area computation is in progress
        return JSONResponse(
            content={
                "result": processing_status,
                "message": "Catchment area computation in progress.",
            },
            status_code=202,
        )
    elif processing_status == ProcessingStatus.success.value:
        # Catchment area computation was successful
        return JSONResponse(
            content={
                "result": processing_status,
                "message": "Catchment area computed successfully.",
            },
            status_code=201,
        )
    elif processing_status == ProcessingStatus.disconnected_origin.value:
        # Catchment area computation failed
        return JSONResponse(
            content={
                "result": processing_status,
                "message": "Starting point(s) are disconnected from the street network.",
            },
            status_code=400,
        )
    else:
        # Catchment area computation failed
        return JSONResponse(
            content={
                "result": processing_status,
                "message": "Failed to compute catchment area.",
            },
            status_code=500,
        )
