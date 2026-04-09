# api/connectors.py
import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Form, Query
from sqlalchemy.orm import Session
from core.db import get_db
from models.credential import Credential
from models.crawl_job import CrawlJob
from models.crawl_run import CrawlRun, RunStatus
from models.connector_type import ConnectorType
from models.connector_field import ConnectorFieldSchema
from tasks.crawl import trigger_crawl
from validators.credential_validators import VALIDATOR_MAP
from pydantic import BaseModel
from typing import Literal
from datetime import datetime

router = APIRouter(prefix="/connectors", tags=["connectors"])

class ManualTriggerRequest(BaseModel):
    run_type: Literal["full", "incremental"] = "incremental"

@router.get("/sources")
def list_sources(db: Session = Depends(get_db)):
    types = db.query(ConnectorType).filter(ConnectorType.is_enabled == True).all()
    return [
        {
            "source_type": ct.source_type,
            "display_name": ct.display_name,
            "auth_type": ct.auth_type,
        }
        for ct in types
    ]


@router.get("/{source_type}/schema")
def get_schema(source_type: str, db: Session = Depends(get_db)):
    ct = db.query(ConnectorType).filter(ConnectorType.source_type == source_type).first()
    if not ct:
        raise HTTPException(404, f"Unknown source_type: {source_type}")

    fields = (
        db.query(ConnectorFieldSchema)
        .filter(ConnectorFieldSchema.connector_type_id == ct.id)
        .order_by(ConnectorFieldSchema.display_order)
        .all()
    )

    credential_fields = {}
    config_fields = {}
    for f in fields:
        field_def = {
            "type": f.field_type,
            "label": f.label,
            "required": f.required,
        }
        if f.default_value is not None:
            field_def["default"] = f.default_value.get("value")
        if f.options:
            field_def["options"] = f.options

        if f.field_category == "credential":
            credential_fields[f.field_name] = field_def
        else:
            config_fields[f.field_name] = field_def

    return {
        "source_type": ct.source_type,
        "display_name": ct.display_name,
        "auth_type": ct.auth_type,
        "oauth_url": ct.oauth_url,
        "credential": credential_fields,
        "config": config_fields,
    }


@router.post("/credentials")
def create_credential(
    source_type: str = Form(...),
    user_id: int = Form(...),
    user_email: str = Form(...),
    credential_json: str = Form("{}"),
    db: Session = Depends(get_db),  
):
    ct = db.query(ConnectorType).filter(ConnectorType.source_type == source_type).first()
    if not ct:
        raise HTTPException(400, f"Unknown source_type: {source_type}")
    cred_data = json.loads(credential_json) if ct.auth_type != "none" else {}

    credential = Credential(
        source_type=source_type,
        credential_json=cred_data,
        user_id=user_id,
        user_email=user_email,
    )
    db.add(credential)
    db.commit()
    db.refresh(credential)
    return {"credential_id": credential.id}

@router.post("/crawl-jobs")
def create_crawl_job(
    credential_id: int = Form(...),
    source_type: str = Form(...),
    config_json: str = Form("{}"),
    name: str | None = Form(None),
    db: Session = Depends(get_db),
):
    ct = db.query(ConnectorType).filter(ConnectorType.source_type == source_type).first()
    if not ct:
        raise HTTPException(400, f"Unknown source_type: {source_type}")

    credential = db.query(Credential).filter(Credential.id == credential_id).first()
    if not credential:
        raise HTTPException(404, "Credential not found")
    if credential.source_type != source_type:
        raise HTTPException(400, f"Credential is for {credential.source_type}, not {source_type}")

    parsed_config = json.loads(config_json)

    if source_type == "google_drive":
        shared_folder_urls = parsed_config.get("shared_folder_urls", "")
        shared_drive_urls = parsed_config.get("shared_drive_urls", "")
        
        parsed_config = {
            "shared_folder_urls": shared_folder_urls,
            "shared_drive_urls": shared_drive_urls,
            "include_shared_drives": False,
            "include_my_drives": False,
            "include_files_shared_with_me": False,
        }
    if source_type == "dropbox":
        raw = parsed_config.get("root_paths") or ""
        if isinstance(raw, str):
            raw = [p.strip() for p in raw.split(",")]
        else:
            raw = [str(p).strip() for p in raw]

        normalized: list[str] = []
        for p in raw:
            if not p:
                continue
            p = p.rstrip("/")
            if not p.startswith("/"):
                p = "/" + p
            normalized.append(p)

        parsed_config = {"root_paths": normalized or [""]}

    
    if source_type == "sharepoint":
        parsed_config = {
            "sites": parsed_config.get("sites", []),
            "excluded_sites": parsed_config.get("excluded_sites", []),
            "excluded_paths": parsed_config.get("excluded_paths", []),
            "include_site_pages": parsed_config.get("include_site_pages", False),
            "include_site_documents": parsed_config.get("include_site_documents", True)
        }

    crawl_job = CrawlJob(
        credential_id=credential_id,
        source_type=source_type,
        config_json=parsed_config,
        is_active=True,
        name=name,
    )
    db.add(crawl_job)
    db.commit()
    db.refresh(crawl_job)

    trigger_crawl.delay(crawl_job.id)
    return {"crawl_job_id": crawl_job.id}


@router.post("/crawl-jobs/{crawl_job_id}/trigger")
def manual_trigger_crawl(
    crawl_job_id: int,
    body: ManualTriggerRequest,
    db: Session = Depends(get_db),
):
    crawl_job = db.query(CrawlJob).filter(CrawlJob.id == crawl_job_id).first()
    if not crawl_job:
        raise HTTPException(404, "Crawl job not found")

    if not crawl_job.is_active:
        raise HTTPException(400, "Crawl job is inactive. Activate it before triggering.")

    active_run = db.query(CrawlRun).filter(
        CrawlRun.crawl_job_id == crawl_job_id,
        CrawlRun.status == RunStatus.RUNNING,
    ).first()

    if active_run:
        active_run.status = RunStatus.CANCELLED
        active_run.completed_at = datetime.utcnow()
        db.commit()
        return {
            "crawl_job_id": crawl_job_id,
            "cancelled_run_id": active_run.id,
            "status": "cancellation_initiated",
            "message": "Active run is being cancelled. Trigger again once the run stops.",
        }

    force_full = body.run_type == "full"
    trigger_crawl.delay(crawl_job_id, force_full_crawl=force_full)
    return {"crawl_job_id": crawl_job_id, "run_type": body.run_type, "status": "triggered"}

@router.get("/credentials")
def get_credentials(
    source_type: str = Query(None),
    db: Session = Depends(get_db)
):
    if source_type:
        credentials = db.query(Credential).filter(Credential.source_type == source_type).all()
    else:
        credentials = db.query(Credential).all()
    return [{"id": c.id, "source_type": c.source_type} for c in credentials]

@router.get("/credentials/by-user")
def get_credentials_by_user(
    user_email: str = Query(...),
    source_type: str = Query(None),
    db: Session = Depends(get_db),
):
    q = db.query(Credential).filter(
        Credential.user_email == user_email,
        Credential.is_deleted == False,
    )
    if source_type:
        q = q.filter(Credential.source_type == source_type)

    credentials = q.order_by(Credential.created_at.desc()).all()

    return [
        {
            "id": c.id,
            "source_type": c.source_type,
            "user_email": c.user_email,
            "is_active": c.is_active,
            "created_at": c.created_at,
            "updated_at": c.updated_at,
        }
        for c in credentials
    ]




@router.get("/credentials/{credential_id}")
def get_credential(credential_id: int, source_type: str = Query(None), db: Session = Depends(get_db)):
    if source_type:
        credential = db.query(Credential).filter(Credential.id == credential_id, Credential.source_type == source_type).first()
    else:
        credential = db.query(Credential).filter(Credential.id == credential_id).first()
    if not credential:
        raise HTTPException(404, "Credential not found")
    return {"id": credential.id, "source_type": credential.source_type}


@router.get("/credentials/{credential_id}/crawl-jobs")
def get_credential_crawl_jobs(credential_id: int, db: Session = Depends(get_db)):
    crawl_jobs = db.query(CrawlJob).filter(CrawlJob.credential_id == credential_id).all()
    return [{"id": c.id, "source_type": c.source_type} for c in crawl_jobs]


@router.get("/crawl-jobs/by-user")
def get_crawl_jobs_by_user(
    user_email: str = Query(...),
    source_type: str = Query(None),
    is_active: bool = Query(None),
    db: Session = Depends(get_db),
):
    q = (
        db.query(CrawlJob)
        .join(Credential, CrawlJob.credential_id == Credential.id)
        .filter(
            Credential.user_email == user_email,
            Credential.is_deleted == False,
        )
    )
    if source_type:
        q = q.filter(CrawlJob.source_type == source_type)
    if is_active is not None:
        q = q.filter(CrawlJob.is_active == is_active)

    crawl_jobs = q.order_by(CrawlJob.created_at.desc()).all()

    return [
        {
            "id": cj.id,
            "credential_id": cj.credential_id,
            "source_type": cj.source_type,
            "config_json": cj.config_json,
            "is_active": cj.is_active,
            "last_run_at": cj.last_run_at,
            "created_at": cj.created_at,
            "consecutive_failures": cj.consecutive_failures,
            "failure_reason": cj.failure_reason,
        }
        for cj in crawl_jobs
    ]


@router.put("/crawl-jobs/{crawl_job_id}")
def update_crawl_job(crawl_job_id: int, db: Session = Depends(get_db)):
    crawl_job = db.query(CrawlJob).filter(CrawlJob.id == crawl_job_id).first()
    if not crawl_job:
        raise HTTPException(404, "Crawl job not found")
    
    if not crawl_job.is_active:
        crawl_job.consecutive_failures = 0
        crawl_job.failure_reason = None
    
    crawl_job.is_active = not crawl_job.is_active
    db.commit()
    db.refresh(crawl_job)
    return {
        "crawl_job_id": crawl_job.id,
        "is_active": crawl_job.is_active,
        "consecutive_failures": crawl_job.consecutive_failures,
        "failure_reason": crawl_job.failure_reason,
    }



@router.post("/admin/clear-data")
def clear_connector_data(db: Session = Depends(get_db)):
    """Truncate crawl_jobs and credentials tables."""
    from sqlalchemy import text

    try:
        db.execute(text("TRUNCATE TABLE crawl_jobs CASCADE"))
        db.execute(text("TRUNCATE TABLE credentials CASCADE"))

        db.commit()
        return {"status": "success", "message": "crawl_jobs and credentials tables truncated"}
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}


@router.post("/validate-credentials")
def validate_credentials(
    source_type: str,
    credentials: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config = config or {}

    if source_type not in VALIDATOR_MAP:
        supported = list(VALIDATOR_MAP.keys())
        return {
            "valid": False,
            "source_type": source_type,
            "api_calls": [],
            "error": f"Unsupported source_type: {source_type}. Supported: {supported}",
            "error_code": "UNSUPPORTED_SOURCE_TYPE",
        }

    validator = VALIDATOR_MAP[source_type]

    if source_type in ("web", "google_drive"):
        result = validator(credentials, config)
    else:
        result = validator(credentials)

    return {
        "valid": result.valid,
        "source_type": result.source_type,
        "api_calls": [
            {
                "url": call.url,
                "method": call.method,
                "status_code": call.status_code,
                "response_summary": call.response_summary,
                "success": call.success,
                "duration_ms": round(call.duration_ms, 2),
            }
            for call in result.api_calls
        ],
        "error": result.error,
        "error_code": result.error_code,
    }


@router.post("/credentials/{credential_id}/validate")
def validate_credential_by_id(
    credential_id: int,
    config: dict[str, Any] | None = None,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    credential = db.query(Credential).filter(Credential.id == credential_id).first()
    if not credential:
        return {
            "valid": False,
            "source_type": "unknown",
            "api_calls": [],
            "error": f"Credential {credential_id} not found",
            "error_code": "NOT_FOUND",
        }

    request_body = {
        "source_type": credential.source_type,
        "credentials": credential.credential_json,
        "config": config or {},
    }

    return validate_credentials(
        source_type=request_body["source_type"],
        credentials=request_body["credentials"],
        config=request_body["config"],
    )

@router.delete("/crawl-jobs/{crawl_job_id}")
def delete_crawl_job(crawl_job_id: int, db: Session = Depends(get_db)):
    crawl_job = db.query(CrawlJob).filter(
        CrawlJob.id == crawl_job_id,
        CrawlJob.is_deleted == False,
    ).first()
    if not crawl_job:
        raise HTTPException(404, "Crawl job not found")

    active_run = db.query(CrawlRun).filter(
        CrawlRun.crawl_job_id == crawl_job_id,
        CrawlRun.status == RunStatus.RUNNING,
    ).first()
    if active_run:
        active_run.status = RunStatus.CANCELLED
        active_run.completed_at = datetime.utcnow()

    crawl_job.is_deleted = True
    crawl_job.is_active = False
    db.commit()
    return {"crawl_job_id": crawl_job_id, "status": "deleted"}


@router.delete("/credentials/{credential_id}")
def delete_credential(credential_id: int, db: Session = Depends(get_db)):
    credential = db.query(Credential).filter(
        Credential.id == credential_id,
        Credential.is_deleted == False,
    ).first()
    if not credential:
        raise HTTPException(404, "Credential not found")

    crawl_jobs = db.query(CrawlJob).filter(
        CrawlJob.credential_id == credential_id,
        CrawlJob.is_deleted == False,
    ).all()
    for job in crawl_jobs:
        active_run = db.query(CrawlRun).filter(
            CrawlRun.crawl_job_id == job.id,
            CrawlRun.status == RunStatus.RUNNING,
        ).first()
        if active_run:
            active_run.status = RunStatus.CANCELLED
            active_run.completed_at = datetime.utcnow()
        job.is_deleted = True
        job.is_active = False

    credential.is_deleted = True
    credential.is_active = False
    db.commit()
    return {"credential_id": credential_id, "status": "deleted"}