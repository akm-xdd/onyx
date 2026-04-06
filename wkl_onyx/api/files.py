# api/files.py
from fastapi import APIRouter, UploadFile, File, HTTPException
from storage import get_storage
from models.crawl_job import CrawlJob
from models.credential import Credential
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
            .filter(CrawlJob.id == crawl_job_id, Credential.user_email == user_email)
            .first()
        )
        if not job:
            raise HTTPException(404, "Crawl job not found")
        prefix = job_prefix(user_email, job.source_type, job.id, job.name)
        return {
            "scope": "crawl_job",
            "crawl_job_id": job.id,
            "name": job.name,
            "source_type": job.source_type,
            "file_count": storage.count(prefix),
        }

    # Single credential
    if credential_id is not None:
        cred = (
            db.query(Credential)
            .filter(Credential.id == credential_id, Credential.user_email == user_email)
            .first()
        )
        if not cred:
            raise HTTPException(404, "Credential not found")
        jobs = db.query(CrawlJob).filter(CrawlJob.credential_id == credential_id).all()
        per_job = []
        total = 0
        for j in jobs:
            c = storage.count(job_prefix(user_email, j.source_type, j.id, j.name))
            per_job.append({"crawl_job_id": j.id, "name": j.name, "source_type": j.source_type, "file_count": c})
            total += c
        return {
            "scope": "credential",
            "credential_id": credential_id,
            "total_file_count": total,
            "crawl_jobs": per_job,
        }

    # All for user
    total = storage.count(f"{_safe_segment(user_email)}/")
    return {
        "scope": "user",
        "user_email": user_email,
        "total_file_count": total,
    }