from core.celery_app import celery_app
from core.db import SessionLocal
from models.crawl_job import CrawlJob
from tasks.crawl import trigger_crawl


@celery_app.task(name="sync_all_jobs")
def sync_all_jobs():
    db = SessionLocal()
    try:
        active_jobs = db.query(CrawlJob).filter(CrawlJob.is_active == True).all()
        for job in active_jobs:
            trigger_crawl.delay(job.id)
    finally:
        db.close()