# api/files.py
import json
from fastapi import APIRouter, UploadFile, File, HTTPException
from storage import get_storage
from models.crawl_job import CrawlJob
from models.crawl_run import CrawlRun
from models.credential import Credential
from models.failed_file import FailedFile, RetryStatus
from core.db import get_db
from sqlalchemy.orm import Session
from fastapi import Query
from fastapi import Depends
from tasks.crawl import job_prefix, _safe_segment

router = APIRouter(prefix="/files", tags=["files"])

ALLOWED_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".txt", ".md", ".csv",
    ".xls", ".xlsx", ".pptx", ".json", ".html", ".rtf",
}


def _get_last_run_info(db: Session, crawl_job_id: int) -> dict:
    """Get the last run status and still-failed files (after retry) for a crawl job."""
    last_run = (
        db.query(CrawlRun)
        .filter(CrawlRun.crawl_job_id == crawl_job_id)
        .order_by(CrawlRun.created_at.desc())
        .first()
    )
    if not last_run:
        return {"last_run_status": None, "failed_files": []}

    failed_rows = (
        db.query(FailedFile)
        .filter(
            FailedFile.crawl_run_id == last_run.id,
            FailedFile.status.in_([RetryStatus.FAILED, RetryStatus.PENDING]),
        )
        .all()
    )
    failed_files = [
        {
            "id": r.file_meta.get("id", "unknown"),
            "name": r.file_meta.get("name", "unknown"),
            "type": r.file_meta.get("mimeType") or r.file_meta.get("mime_type", "unknown"), # google is mimeType, dropbox is mime_type
            "error": r.error_message,
        }
        for r in failed_rows
    ]

    return {
        "last_run_status": last_run.status,
        "failed_files": failed_files,
    }


def _safe_filename(name: str) -> str:
    for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '#']:
        name = name.replace(char, "_")
    return name


@router.post("/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    storage = get_storage()
    uploaded = []
    errors = []

    for file in files:
        filename = file.filename or "unknown"
        ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

        if ext not in ALLOWED_EXTENSIONS:
            errors.append({"file": filename, "error": f"Extension {ext} not allowed"})
            continue

        try:
            content = await file.read()
            safe_name = _safe_filename(filename)
            storage_key = f"uploads/{safe_name}"

            storage.upload(storage_key, content)
            uploaded.append({"file": filename, "storage_key": storage_key})
        except Exception as e:
            errors.append({"file": filename, "error": str(e)})

    return {
        "uploaded": uploaded,
        "errors": errors,
        "total_uploaded": len(uploaded),
        "total_errors": len(errors),
    }

@router.get("/status")
def get_status(
    user_email: str = Query(...),
    credential_id: int | None = Query(None),
    crawl_job_id: int | None = Query(None),
    db: Session = Depends(get_db),
):
    storage = get_storage()

    # Single job
    if crawl_job_id is not None:
        job = (
            db.query(CrawlJob)
            .join(Credential, CrawlJob.credential_id == Credential.id)
            .filter(
                CrawlJob.id == crawl_job_id,
                Credential.user_email == user_email,
                CrawlJob.is_deleted == False,
                Credential.is_deleted == False,
            )
            .first()
        )
        if not job:
            raise HTTPException(404, "Crawl job not found")
        prefix = job_prefix(user_email, job.source_type, job.id, job.name)
        run_info = _get_last_run_info(db, job.id)
        return {
            "scope": "crawl_job",
            "crawl_job_id": job.id,
            "name": job.name,
            "source_type": job.source_type,
            "file_count": storage.count(prefix),
            "last_run_status": run_info["last_run_status"],
            "failed_files": run_info["failed_files"],
        }

    # Single credential
    if credential_id is not None:
        cred = (
            db.query(Credential)
            .filter(
                Credential.id == credential_id,
                Credential.user_email == user_email,
                Credential.is_deleted == False,
            )
            .first()
        )
        if not cred:
            raise HTTPException(404, "Credential not found")
        jobs = db.query(CrawlJob).filter(
            CrawlJob.credential_id == credential_id,
            CrawlJob.is_deleted == False,
        ).all()
        per_job = []
        total = 0
        for j in jobs:
            c = storage.count(job_prefix(user_email, j.source_type, j.id, j.name))
            run_info = _get_last_run_info(db, j.id)
            per_job.append({
                "crawl_job_id": j.id,
                "name": j.name,
                "source_type": j.source_type,
                "file_count": c,
                "last_run_status": run_info["last_run_status"],
                "failed_files": run_info["failed_files"],
            })
            total += c
        return {
            "scope": "credential",
            "credential_id": credential_id,
            "total_file_count": total,
            "crawl_jobs": per_job,
        }

    # All for user
    user_prefix = f"{_safe_segment(user_email)}/"
    
    credentials = db.query(Credential).filter(
        Credential.user_email == user_email,
        Credential.is_deleted == False,
    ).all()
    source_type_counts: dict[str, int] = {}
    credential_info: list[dict] = []
    total = 0
    
    for cred in credentials:
        jobs = db.query(CrawlJob).filter(
            CrawlJob.credential_id == cred.id,
            CrawlJob.is_deleted == False,
        ).all()
        cred_total = 0
        job_details = []
        
        for j in jobs:
            prefix = job_prefix(user_email, j.source_type, j.id, j.name)
            count = storage.count(prefix)
            run_info = _get_last_run_info(db, j.id)
            job_details.append({
                "crawl_job_id": j.id,
                "name": j.name,
                "source_type": j.source_type,
                "file_count": count,
                "last_run_status": run_info["last_run_status"],
                "failed_files": run_info["failed_files"],
            })
            cred_total += count
            if j.source_type not in source_type_counts:
                source_type_counts[j.source_type] = 0
            source_type_counts[j.source_type] += count
        
        credential_info.append({
            "credential_id": cred.id,
            "source_type": cred.source_type,
            "file_count": cred_total,
            "crawl_jobs": job_details
        })
        total += cred_total
    
    sources = [
        {"source_type": st, "file_count": count}
        for st, count in sorted(source_type_counts.items())
    ]
    
    return {
        "scope": "user",
        "user_email": user_email,
        "total_file_count": total,
        "sources": sources,
        "credentials": credential_info,
    }