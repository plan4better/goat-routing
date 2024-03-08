import asyncio

from celery import Celery
from redis import Redis

from src.core.config import settings
from src.crud.crud_catchment_area import CRUDCatchmentArea
from src.db.session import async_session

celery_app = Celery("worker", broker=settings.CELERY_BROKER_URL)
redis = Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
)
crud_catchment_area = CRUDCatchmentArea(async_session(), redis)


@celery_app.task
def run_catchment_area(params):
    loop = asyncio.get_event_loop()
    coroutine = crud_catchment_area.run(params)
    loop.run_until_complete(coroutine)
    return "OK"
