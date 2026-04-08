import json
import os
import sys
from datetime import datetime, timedelta, timezone

import redis
from core.celery_app import celery_app
from core.config import settings
from core.db import SessionLocal
from models.credential import Credential
from models.crawl_job import CrawlJob
from models.crawl_run import CrawlRun, RunStatus, RunType
from models.checkpoint import Checkpoint
from storage import get_storage

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_redis = redis.from_url(settings.REDIS_URL)

# will move to env
LOCK_TTL = 2 * 3600
STALE_RUN_TIMEOUT_HOURS = 2


def _safe_segment(name: str) -> str:
    return name.strip().replace("/", "_").replace(":", "_").replace("?", "_").replace("#", "_") or "_"

def job_prefix(user_email: str, source_type: str, job_id: int, job_name: str | None) -> str:
    job_segment = _safe_segment(job_name) if job_name else str(job_id)
    return f"{_safe_segment(user_email)}/{source_type}/{job_segment}/"

def _upload_files(crawler, files, source_type, job_id, job_name, user_email, storage):
    count = 0
    errors = 0
    failed_files: list[dict] = []
    job_segment = _safe_segment(job_name) if job_name else str(job_id)
    email_segment = _safe_segment(user_email)
    for file_meta in files:
        try:
            content, filename, metadata = crawler.download(file_meta)
            if not content:
                print(f"[trigger_crawl] Failed to process {file_meta.get('name')}: No content")
                errors += 1
                failed_files.append({
                    "id": file_meta.get("id", "unknown"),
                    "name": file_meta.get("name", "unknown"),
                    "type": file_meta.get("type") or file_meta.get("mime_type", "unknown"),
                })
                continue
            folder_path = metadata.get("folder_path", [])
            folder_prefix = (
                "/".join(_safe_segment(p) for p in folder_path)
                if folder_path
                else _safe_segment(file_meta["id"])
            )
            storage_key = f"{email_segment}/{source_type}/{job_segment}/{folder_prefix}/{filename}"
            storage.upload(storage_key, content)
            print(f"[trigger_crawl] Uploaded: {storage_key}")
            count += 1
        except Exception as e:
            print(f"[trigger_crawl] Failed to process {file_meta.get('name')}: {e}")
            errors += 1
            failed_files.append({
                "id": file_meta.get("id", "unknown"),
                "name": file_meta.get("name", "unknown"),
                "type": file_meta.get("type") or file_meta.get("mime_type", "unknown"),
            })
    return count, errors, failed_files

@celery_app.task(bind=True, name="trigger_crawl")
def trigger_crawl(self, crawl_job_id: int):

    with SessionLocal() as db:
        _recover_stale_runs(db, crawl_job_id)

    lock_key = f"crawl_lock:{crawl_job_id}"
    if not _redis.set(lock_key, "1", nx=True, ex=LOCK_TTL):
        print(f"[trigger_crawl] Job {crawl_job_id} already locked, skipping")
        return

    run_id = None
    try:
        # init crawler, create run
        with SessionLocal() as db:
            crawl_job = db.query(CrawlJob).filter(CrawlJob.id == crawl_job_id).first()
            if not crawl_job:
                return
            credential = db.query(Credential).filter(Credential.id == crawl_job.credential_id).first()
            if not credential:
                return

            active = db.query(CrawlRun).filter(
                CrawlRun.crawl_job_id == crawl_job_id,
                CrawlRun.status == RunStatus.RUNNING,
            ).first()
            if active:
                print(f"[trigger_crawl] Job {crawl_job_id} already has an active run, skipping")
                return

            last_successful = (
                db.query(CrawlRun)
                .filter(
                    CrawlRun.crawl_job_id == crawl_job_id,
                    CrawlRun.status.in_([RunStatus.SUCCESS, RunStatus.COMPLETED_WITH_ERRORS]),
                )
                .order_by(CrawlRun.completed_at.desc())
                .first()
            )
            is_incremental = last_successful is not None
            start_time = last_successful.completed_at.replace(tzinfo=timezone.utc).timestamp() if is_incremental else 0

            checkpoint_data = _get_checkpoint(db, crawl_job_id, is_incremental)

            run = CrawlRun(
                crawl_job_id=crawl_job_id,
                status=RunStatus.RUNNING,
                run_type=RunType.INCREMENTAL if is_incremental else RunType.FULL,
                started_at=datetime.utcnow(),
            )
            db.add(run)
            db.commit()
            db.refresh(run)
            run_id = run.id
            source_type = crawl_job.source_type
            job_id = crawl_job.id
            job_name = crawl_job.name
            user_email = credential.user_email

            print(f"[trigger_crawl] {'Incremental' if is_incremental else 'Full'} crawl from {datetime.utcfromtimestamp(start_time)}")
            print(f"[trigger_crawl] Created run id={run_id} type={run.run_type}")

            backend_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../backend"))
            if backend_path not in sys.path:
                sys.path.insert(0, backend_path)

            from connectors.loader import get_crawler

            crawler = get_crawler(crawl_job.source_type, crawl_job, credential)

        # will move to singleton
        storage = get_storage()

        total_count, total_errors, all_failed_files = _run_crawl_loop(
            crawler, source_type, job_id, job_name, user_email, storage,
            crawl_job_id, checkpoint_data, start_time=0 if not is_incremental else start_time
        )

        # write final status
        with SessionLocal() as db:
            run = db.query(CrawlRun).filter(CrawlRun.id == run_id).first()
            if total_errors == 0:
                run.status = RunStatus.SUCCESS
            else:
                failed_file_names = [f["name"] for f in all_failed_files]
                if total_count == 0:
                    run.status = RunStatus.FAILED
                    if len(failed_file_names) <= 5:
                        human_message = f"All {total_errors} files failed: {', '.join(failed_file_names)}"
                    else:
                        human_message = f"All {total_errors} files failed: {', '.join(failed_file_names[:5])}, ..."
                else:
                    run.status = RunStatus.COMPLETED_WITH_ERRORS
                    if len(failed_file_names) <= 5:
                        human_message = f"{total_errors} of {total_count + total_errors} files failed: {', '.join(failed_file_names)}"
                    else:
                        human_message = f"{total_errors} of {total_count + total_errors} files failed: {', '.join(failed_file_names[:5])}, ..."
                error_data = {
                    "message": human_message,
                    "failed_files": all_failed_files
                }
                run.error = json.dumps(error_data)
            run.completed_at = datetime.utcnow()
            run.docs_processed = total_count

            # update last run at
            job = db.query(CrawlJob).filter(CrawlJob.id == crawl_job_id).first()
            if job:
                job.last_run_at = datetime.utcnow()

                try:
                    if run.status in (RunStatus.SUCCESS, RunStatus.COMPLETED_WITH_ERRORS):
                        job.consecutive_failures = 0
                        job.failure_reason = None
                    elif run.status == RunStatus.FAILED:
                        job.consecutive_failures = (job.consecutive_failures or 0) + 1
                        job.failure_reason = run.error[:500] if run.error else "Unknown error"

                        if job.consecutive_failures >= settings.MAX_CONSECUTIVE_FAILURES:
                            job.is_active = False
                            print(f"[trigger_crawl] Job {crawl_job_id} disabled after {job.consecutive_failures} consecutive failures: {job.failure_reason}")

                    # disable web scraping task after successful run
                    if source_type == "web" and run.status == RunStatus.SUCCESS:
                        job.is_active = False
                except AttributeError:
                    pass

            db.commit()
            print(f"[trigger_crawl] Run {run_id} → {run.status} ({total_count} uploaded, {total_errors} errors)")

            if run.status == RunStatus.FAILED and job.is_active and job.consecutive_failures < settings.MAX_CONSECUTIVE_FAILURES:
                print(f"[trigger_crawl] Retrying job {crawl_job_id} immediately (attempt {job.consecutive_failures + 1})")
                trigger_crawl.delay(crawl_job_id)

    except Exception as e:
        print(f"[trigger_crawl] Fatal error: {e}")
        if run_id:
                with SessionLocal() as db:
                    run = db.query(CrawlRun).filter(CrawlRun.id == run_id).first()
                    if run:
                        run.status = RunStatus.FAILED
                        run.error = str(e)[:500]
                        run.completed_at = datetime.utcnow()
                        db.commit()

                    try:
                        job = db.query(CrawlJob).filter(CrawlJob.id == crawl_job_id).first()
                        if job:
                            job.consecutive_failures = (job.consecutive_failures or 0) + 1
                            job.failure_reason = str(e)[:500]
                            if job.consecutive_failures >= settings.MAX_CONSECUTIVE_FAILURES:
                                job.is_active = False
                                print(f"[trigger_crawl] Job {crawl_job_id} disabled after {job.consecutive_failures} consecutive failures: {job.failure_reason}")
                            db.commit()
                        
                        if job.is_active and job.consecutive_failures < settings.MAX_CONSECUTIVE_FAILURES:
                            print(f"[trigger_crawl] Retrying job {crawl_job_id} after fatal error (attempt {job.consecutive_failures + 1})")
                            trigger_crawl.delay(crawl_job_id)
                    except AttributeError:
                        pass
        raise
    finally:
        _redis.delete(lock_key)


def _recover_stale_runs(db, crawl_job_id: int) -> None:
    cutoff = datetime.utcnow() - timedelta(hours=STALE_RUN_TIMEOUT_HOURS)
    stale = (
        db.query(CrawlRun)
        .filter(
            CrawlRun.crawl_job_id == crawl_job_id,
            CrawlRun.status == RunStatus.RUNNING,
            CrawlRun.started_at < cutoff,
        )
        .all()
    )
    for run in stale:
        run.status = RunStatus.FAILED
        run.error = "Timed out - worker likely crashed"
        run.completed_at = datetime.utcnow()
        _redis.delete(f"crawl_lock:{crawl_job_id}")
        print(f"[recover_stale_runs] Marked run {run.id} as failed (stale), cleared lock")
    if stale:
        db.commit()


def _get_checkpoint(db, crawl_job_id: int, is_incremental: bool) -> dict | None:
    if is_incremental:
        return None
    
    # For full crawl resumption
    last = (
        db.query(Checkpoint)
        .filter(Checkpoint.crawl_job_id == crawl_job_id)
        .order_by(Checkpoint.created_at.desc())
        .first()
    )
    return dict(last.checkpoint_json) if last else None


def _run_crawl_loop(crawler, source_type, job_id, job_name, user_email, storage, crawl_job_id, checkpoint_data, start_time):
    total_count = 0
    total_errors = 0
    all_failed_files: list[dict] = []

    while True:
        files, next_checkpoint = crawler.fetch_files(checkpoint_data, start=start_time)
        count, errors, failed_files = _upload_files(crawler, files, source_type, job_id, job_name, user_email, storage)
        total_count += count
        total_errors += errors
        all_failed_files.extend(failed_files)

        with SessionLocal() as db:
            db.add(Checkpoint(crawl_job_id=crawl_job_id, checkpoint_json=next_checkpoint))
            db.commit()

        checkpoint_data = next_checkpoint

        if source_type == "google_drive":
            from onyx.connectors.google_drive.models import GoogleDriveCheckpoint
            ckpt = GoogleDriveCheckpoint.model_validate(next_checkpoint)
            if ckpt.completion_stage.value == "done" or not ckpt.has_more:
                break
        else:
            if len(files) == 0 or not next_checkpoint.get("has_more", False):
                break

    return total_count, total_errors, all_failed_files