from celery import Celery
from core.config import settings

celery_app = Celery(
    "wkl_onyx",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["tasks.crawl", "tasks.sync"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    worker_pool='solo',
    beat_schedule={
        "sync-all-active-crawl-jobs": {
            "task": "sync_all_jobs",
            "schedule": 300,
        }
    },
)