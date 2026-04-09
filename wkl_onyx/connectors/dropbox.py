from datetime import timezone

from dropbox.files import FileMetadata  # type: ignore[import-untyped]

from connectors.base import BaseCrawler
from core.config import settings
from onyx.connectors.dropbox.connector import DropboxConnector
from dropbox import Dropbox
import logging

logger = logging.getLogger(__name__)


class DropboxCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        cred = credential.credential_json

        self._dbx = Dropbox(
            oauth2_access_token=cred["dropbox_access_token"],
            oauth2_refresh_token=cred["dropbox_refresh_token"],
            app_key=settings.DROPBOX_APP_KEY,
            app_secret=settings.DROPBOX_APP_SECRET,
        )

        self.connector = DropboxConnector(batch_size=self.batch_size)
        self.connector.dropbox_client = self._dbx

    @property
    def _client(self):
        return self._dbx

    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        items: list[dict] = []
        skipped = 0

        cursor = checkpoint.get("cursor") if checkpoint else None

        if cursor:
            result = self._client.files_list_folder_continue(cursor)
        else:
            result = self._client.files_list_folder(
                "",
                recursive=True,
                include_non_downloadable_files=False,
            )

        for entry in result.entries:
            if not isinstance(entry, FileMetadata):
                continue

            if start > 0 and entry.client_modified:
                modified = entry.client_modified
                if modified.tzinfo is None:
                    modified = modified.replace(tzinfo=timezone.utc)
                if modified.timestamp() < start:
                    continue

            if entry.size and entry.size > self.max_file_size_bytes:
                logger.warning(f"[DropboxCrawler] Skipping large file: {entry.name} ({entry.size / 1024 / 1024:.1f} MB)")
                skipped += 1
                continue

            items.append({
                "id": entry.id,
                "name": entry.name,
                "path_display": entry.path_display,
                "size": entry.size,
                "client_modified": (
                    entry.client_modified.isoformat() if entry.client_modified else None
                ),
            })

        logger.info(f"[DropboxCrawler] Page returned {len(items)} files, skipped {skipped} large files, has_more={result.has_more}")

        return items, {"cursor": result.cursor, "has_more": result.has_more}


    def download(self, file_meta: dict) -> tuple[bytes, str, dict]:
        """Download a single file's content from Dropbox via the SDK.
        """
        path = file_meta["path_display"]
        name = file_meta.get("name", "unknown")

        logger.info(f"[DropboxCrawler] Downloading: {name} ({file_meta.get('size', 'unknown')} bytes)")

        # Actual download happens HERE — not during listing
        content = self.connector._download_file(path)

        # Sanitize filename
        filename = name
        for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '#']:
            filename = filename.replace(char, "_")

        # Build metadata with folder path
        metadata: dict = {
            "source": "dropbox",
            "client_modified": file_meta.get("client_modified"),
            "size": file_meta.get("size"),
        }

        # Extract folder path from path_display  (e.g. "/Projects/Q1/report.pdf")
        if path:
            parts = [p for p in path.split("/") if p]
            if len(parts) > 1:
                metadata["folder_path"] = parts[:-1]
                metadata["folder_path_str"] = " / ".join(parts[:-1])
            else:
                metadata["folder_path"] = []
                metadata["folder_path_str"] = ""

        logger.info(f"[DropboxCrawler] Done: {filename}, folder: {metadata.get('folder_path_str', '')}")
        return content, filename, metadata