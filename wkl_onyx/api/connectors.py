# api/connectors.py
import json
from fastapi import APIRouter, Depends, HTTPException, Form, Query
from sqlalchemy.orm import Session
from core.db import get_db
from models.credential import Credential
from models.crawl_job import CrawlJob
from models.connector_type import ConnectorType
from models.connector_field import ConnectorFieldSchema
from tasks.crawl import trigger_crawl

router = APIRouter(prefix="/connectors", tags=["connectors"])


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
        folder_urls = parsed_config.get("folder_urls", [])
        shared_drive_urls = parsed_config.get("shared_drive_urls", [])
        
        parsed_config = {
            "shared_folder_urls": ",".join(folder_urls) if folder_urls else None,
            "shared_drive_urls": ",".join(shared_drive_urls) if shared_drive_urls else None,
            "include_shared_drives": False,
            "include_my_drives": False,
            "include_files_shared_with_me": False,
        }
        
        print("parsed_config: \n", parsed_config)
    
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
    )
    db.add(crawl_job)
    db.commit()
    db.refresh(crawl_job)

    trigger_crawl.delay(crawl_job.id)
    return {"crawl_job_id": crawl_job.id}


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

@router.put("/crawl-jobs/{crawl_job_id}")
def update_crawl_job(crawl_job_id: int, db: Session = Depends(get_db)):
    crawl_job = db.query(CrawlJob).filter(CrawlJob.id == crawl_job_id).first()
    if not crawl_job:
        raise HTTPException(404, "Crawl job not found")
    crawl_job.is_active = not crawl_job.is_active
    db.commit()
    db.refresh(crawl_job)
    return {"crawl_job_id": crawl_job.id, "is_active": crawl_job.is_active}