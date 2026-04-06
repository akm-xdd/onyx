# connectors/sharepoint.py

import io
import time
from datetime import datetime, timezone

from connectors.base import BaseCrawler
from core.config import settings
from onyx.connectors.sharepoint.connector import (
    SharepointConnector,
    SharepointConnectorCheckpoint,
    DriveItemData,
    _build_item_relative_path,
    SHARED_DOCUMENTS_MAP,
)
from onyx.file_processing.extract_file_text import get_file_ext
from onyx.file_processing.file_types import OnyxFileExtensions

DEFAULT_BATCH_SIZE = 50


class SharepointCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        config = crawl_job.config_json or {}

        self.connector = SharepointConnector(
            sites=config.get("sites", []),
            excluded_sites=config.get("excluded_sites", []),
            excluded_paths=config.get("excluded_paths", []),
            include_site_pages=config.get("include_site_pages", False),
            include_site_documents=config.get("include_site_documents", True),
        )

        cred_json = dict(credential.credential_json)
        self.connector.load_credentials(cred_json)

    def fetch_files(
        self, checkpoint: dict | None, start: float = 0
    ) -> tuple[list[dict], dict]:
        if checkpoint:
            ckpt = SharepointConnectorCheckpoint.model_validate(checkpoint)
            if not ckpt.has_more:
                ckpt = self.connector.build_dummy_checkpoint()
        else:
            ckpt = self.connector.build_dummy_checkpoint()

        batch_size = self.crawl_job.config_json.get("batch_size", DEFAULT_BATCH_SIZE)
        max_size = settings.MAX_FILE_SIZE_MB * 1024 * 1024

        start_dt = datetime.fromtimestamp(start, tz=timezone.utc) if start > 0 else None
        end_dt = datetime.now(tz=timezone.utc)

        site_descriptors = self.connector._filter_excluded_sites(
            self.connector.site_descriptors or self.connector.fetch_sites()
        )

        files: list[dict] = []
        skipped = 0

        for site_descriptor in site_descriptors:
            if len(files) >= batch_size:
                break

            for driveitem, drive_name, drive_web_url in self.connector._fetch_driveitems(
                site_descriptor=site_descriptor,
                start=start_dt,
                end=end_dt,
            ):
                if self.connector._is_driveitem_excluded(driveitem):
                    continue

                # Skip unsupported extensions
                file_ext = get_file_ext(driveitem.name)
                if file_ext not in OnyxFileExtensions.ALL_ALLOWED_EXTENSIONS:
                    continue

                # Skip oversized files
                if driveitem.size and driveitem.size > max_size:
                    print(
                        f"[SharepointCrawler] Skipping large file: {driveitem.name} "
                        f"({driveitem.size / 1024 / 1024:.1f} MB)"
                    )
                    skipped += 1
                    continue

                # Build folder path from parentReference
                folder_path = self.connector._extract_folder_path_from_parent_reference(
                    driveitem.parent_reference_path
                )

                files.append(
                    {
                        "id": driveitem.id,
                        "name": driveitem.name,
                        "web_url": driveitem.web_url,
                        "mime_type": driveitem.mime_type,
                        "size": driveitem.size,
                        "download_url": driveitem.download_url,
                        "drive_id": driveitem.drive_id,
                        "drive_name": drive_name,
                        "drive_web_url": drive_web_url,
                        "site_url": site_descriptor.url,
                        "folder_path": folder_path,
                        "last_modified": (
                            driveitem.last_modified_datetime.isoformat()
                            if driveitem.last_modified_datetime
                            else None
                        ),
                    }
                )

                if len(files) >= batch_size:
                    break

        print(
            f"[SharepointCrawler] Fetched {len(files)} files, skipped {skipped} large files"
        )

        # For SharePoint we don't use the SDK checkpoint for crawling —
        # we rely on the time-based start param for incremental syncs.
        # Return a simple checkpoint that tracks completion.
        next_checkpoint = {"has_more": False}
        return files, next_checkpoint

    def download(self, file_meta: dict) -> tuple[bytes, str, dict]:
        file_name = file_meta["name"]
        download_url = file_meta.get("download_url")
        drive_id = file_meta.get("drive_id")
        item_id = file_meta["id"]
        max_size = settings.MAX_FILE_SIZE_MB * 1024 * 1024

        print(f"[SharepointCrawler] Downloading: {file_name} ({f'{file_meta.get("size", 0) / 1024 / 1024:.1f} MB' if file_meta.get('size') else 'unknown size'})")
        content: bytes | None = None

        # Try downloadUrl first
        if download_url:
            try:
                from onyx.connectors.sharepoint.connector import _download_with_cap, SizeCapExceeded
                content = _download_with_cap(download_url, 60, max_size)
            except SizeCapExceeded:
                print(f"[SharepointCrawler] File exceeds size cap: {file_name}")
                return b"", file_name, {}
            except Exception as e:
                print(f"[SharepointCrawler] downloadUrl failed for {file_name}: {e}, trying Graph API")

        # Fallback to Graph API
        if content is None and drive_id:
            try:
                from onyx.connectors.sharepoint.connector import _download_via_graph_api, SizeCapExceeded
                access_token = self.connector._get_graph_access_token()
                content = _download_via_graph_api(
                    access_token, drive_id, item_id, max_size,
                    graph_api_base=self.connector.graph_api_base,
                )
            except SizeCapExceeded:
                print(f"[SharepointCrawler] File exceeds size cap via Graph API: {file_name}")
                return b"", file_name, {}
            except Exception as e:
                print(f"[SharepointCrawler] Graph API download failed for {file_name}: {e}")
                return b"", file_name, {}

        if not content:
            print(f"[SharepointCrawler] No content for {file_name}")
            return b"", file_name, {}

        # Build folder path metadata
        folder_path_parts: list[str] = []
        site_url = file_meta.get("site_url", "")
        site_name = site_url.rstrip("/").split("/")[-1] if site_url else ""
        if site_name:
            folder_path_parts.append(site_name)

        drive_name = file_meta.get("drive_name", "")
        if drive_name:
            folder_path_parts.append(drive_name)

        folder_path = file_meta.get("folder_path")
        if folder_path:
            folder_path_parts.extend(folder_path.split("/"))

        metadata = {
            "folder_path": folder_path_parts,
            "folder_path_str": " / ".join(folder_path_parts),
            "site_url": site_url,
            "drive_name": drive_name,
            "web_url": file_meta.get("web_url", ""),
        }

        print(f"[SharepointCrawler] Done: {file_name}, folder: {metadata['folder_path_str']}")
        return content, file_name, metadata