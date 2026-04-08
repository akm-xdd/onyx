from onyx.connectors.google_drive.doc_conversion import download_request
import json
import time


from connectors.base import BaseCrawler
from onyx.connectors.google_drive.connector import GoogleDriveConnector
from onyx.connectors.google_drive.file_retrieval import DriveFileFieldType
from onyx.connectors.google_drive.models import GDriveMimeType
from onyx.connectors.google_utils.resources import get_drive_service
from onyx.connectors.google_drive.doc_conversion import (
    build_folder_path,
    GOOGLE_MIME_TYPES_TO_EXPORT,
)
from core.config import settings

import logging

logger = logging.getLogger(__name__)

DRIVE_FOLDER_TYPE = "application/vnd.google-apps.folder"
DRIVE_SHORTCUT_TYPE = "application/vnd.google-apps.shortcut"

GOOGLE_EXPORT_MAP = {
    GDriveMimeType.DOC.value: ("text/plain", ".txt"),
    GDriveMimeType.SPREADSHEET.value: ("text/csv", ".csv"),
    GDriveMimeType.PPT.value: ("text/plain", ".txt"),
}

DEFAULT_BATCH_SIZE = 50


class GoogleDriveCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        logger.info(f"[GoogleDriveCrawler] config: {crawl_job.config_json}")
        self.connector = GoogleDriveConnector(
            include_my_drives=crawl_job.config_json.get("include_my_drives", True),
            include_shared_drives=crawl_job.config_json.get("include_shared_drives", False),
            include_files_shared_with_me=crawl_job.config_json.get("include_files_shared_with_me", False),
            shared_folder_urls=crawl_job.config_json.get("shared_folder_urls"),
            my_drive_emails=crawl_job.config_json.get("my_drive_emails"),
            shared_drive_urls=crawl_job.config_json.get("shared_drive_urls"),
        )
        cred_json = dict(credential.credential_json)
        if isinstance(cred_json.get("google_tokens"), dict):
            tokens = dict(cred_json["google_tokens"])
            tokens.setdefault("expiry", None)
            tokens.setdefault("universe_domain", "googleapis.com")
            tokens.setdefault("account", "")
            cred_json["google_tokens"] = json.dumps(tokens)
        self.connector.load_credentials(cred_json)



    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        if checkpoint:
            from onyx.connectors.google_drive.models import GoogleDriveCheckpoint
            ckpt = GoogleDriveCheckpoint.model_validate(checkpoint)
            if ckpt.completion_stage.value == "done" or not ckpt.has_more:
                ckpt = self.connector.build_dummy_checkpoint()
        else:
            ckpt = self.connector.build_dummy_checkpoint()

        batch_size = self.crawl_job.config_json.get("batch_size", DEFAULT_BATCH_SIZE)
        max_size_bytes = settings.MAX_FILE_SIZE_MB * 1024 * 1024
        files = []
        skipped = 0

        logger.info(f"[fetch_files] checkpoint stage: {ckpt.completion_stage}, start={start}")

        generator = self.connector._fetch_drive_items(
            field_type=DriveFileFieldType.STANDARD,
            checkpoint=ckpt,
            start=start,
            end=time.time(),
        )

        for retrieved_file in generator:
            if retrieved_file.error:
                logger.exception(f"[fetch_files] error on item: {retrieved_file.error}")
                continue

            mime = retrieved_file.drive_file.get("mimeType", "")
            if mime in [DRIVE_FOLDER_TYPE, DRIVE_SHORTCUT_TYPE]:
                continue

            size_str = retrieved_file.drive_file.get("size")
            if size_str:
                size = int(size_str)
                if size > max_size_bytes:
                    logger.warning(f"[fetch_files] Skipping large file: {retrieved_file.drive_file.get('name')} ({size / 1024 / 1024:.1f} MB)")
                    skipped += 1
                    continue

            files.append({
                "id": retrieved_file.drive_file.get("id"),
                "name": retrieved_file.drive_file.get("name", "unknown"),
                "mimeType": mime,
                "user_email": retrieved_file.user_email,
                "size": int(size_str) if size_str else None,
                "parents": retrieved_file.drive_file.get("parents", []),
                "driveId": retrieved_file.drive_file.get("driveId"),
                "owners": retrieved_file.drive_file.get("owners", []),
            })

            if len(files) >= batch_size:
                break

        logger.info(f"[fetch_files] Fetched {len(files)} files, skipped {skipped} large files")
        next_checkpoint = ckpt.model_dump(mode="json")
        return files, next_checkpoint


    def download(self, file_meta: dict) -> tuple[bytes, str, dict]:
        service = get_drive_service(self.connector.creds, file_meta["user_email"])
        file_id = file_meta["id"]
        file_name = file_meta["name"]
        mime_type = file_meta["mimeType"]
        size = file_meta.get("size")
        max_size = settings.MAX_FILE_SIZE_MB * 1024 * 1024

        logger.info(f"[download] Starting: {file_name} ({f'{size / 1024 / 1024:.1f} MB' if size else 'unknown size'})")

        if mime_type in GOOGLE_MIME_TYPES_TO_EXPORT:
            export_mime = GOOGLE_MIME_TYPES_TO_EXPORT[mime_type]
            request = service.files().export_media(fileId=file_id, mimeType=export_mime)
            from onyx.connectors.google_drive.doc_conversion import _download_request
            content = _download_request(request, file_id, max_size)
            file_name = file_name + ".txt"
        else:
            content = download_request(service, file_id, max_size)

        # Resolve the human-readable folder path (e.g. ["My Drive", "Projects", "Q1"])
        folder_path = build_folder_path(
            file=file_meta,
            service=service,
            drive_id=file_meta.get("driveId"),
            user_email=file_meta["user_email"],
        )
        metadata = {
            "folder_path": folder_path,             # list of folder names, root → parent
            "folder_path_str": " / ".join(folder_path),  # human-readable string
        }

        logger.info(f"[download] Done: {file_name}, folder: {metadata['folder_path_str']}")
        return content, file_name, metadata