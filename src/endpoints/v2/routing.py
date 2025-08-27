import json

import httpx
from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import JSONResponse
from redis import Redis

from src.core.config import settings
from src.core.worker import run_catchment_area
from src.schemas.ab_routing import IMotisPlan, motis_request_examples
from src.schemas.catchment_area import (
    ICatchmentAreaActiveMobility,
    ICatchmentAreaCar,
    request_examples,
)
from src.schemas.status import ProcessingStatus

router = APIRouter()
redis = Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
)


@router.post(
    "/active-mobility/catchment-area",
    summary="Compute catchment areas for active mobility",
)
async def compute_active_mobility_catchment_area(
    *,
    params: ICatchmentAreaActiveMobility = Body(
        ...,
        examples=request_examples["catchment_area_active_mobility"],
        description="The catchment area parameters.",
    ),
):
    """Compute catchment areas for active mobility."""

    return await compute_catchment_area(params)


@router.post(
    "/motorized-mobility/catchment-area",
    summary="Compute catchment areas for motorized mobility",
)
async def compute_motorized_mobility_catchment_area(
    *,
    params: ICatchmentAreaCar = Body(
        ...,
        examples=request_examples["catchment_area_motorized_mobility"],
        description="The catchment area parameters.",
    ),
):
    """Compute catchment areas for motorized mobility."""

    return await compute_catchment_area(params)


async def compute_catchment_area(
    params: ICatchmentAreaActiveMobility | ICatchmentAreaCar,
):
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


@router.post(
    "/ab-routing",
    summary="Compute AB-routing using motis",
)
async def compute_ab_routing(
    params: IMotisPlan = Body(
        ...,
        example=motis_request_examples["default"],
        description="The motis plan service required parameters.",
    ),
):
    """
    Compute a routing plan by forwarding a request to the motis service.
    """
    return await compute_motis_request(params)


async def compute_motis_request(params: IMotisPlan):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                settings.MOTIS_PLAN_ENDPOINT, params=params.dict()
            )
            response.raise_for_status()

            return JSONResponse(
                content={
                    "result": response.json(),
                    "message": "Plan computed successfully.",
                },
                status_code=200,
            )

        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Error from motis service: {e.response.text}",
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=503, detail=f"Cannot connect to motis service: {e}"
            )
