from fastapi import APIRouter
from fastapi.responses import JSONResponse
router = APIRouter()

@router.get("/healthz", summary="Health Check", response_class=JSONResponse, status_code=200)
def ping():
    """Health check."""
    return {"ping": "pong!"}
