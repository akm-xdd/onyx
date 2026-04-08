from datetime import timezone

from dropbox.files import FileMetadata  # type: ignore[import-untyped]

from connectors.base import BaseCrawler
from core.config import settings
from onyx.connectors.dropbox.connector import DropboxConnector
import logging

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100


class DropboxCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        config = crawl_job.config_json or {}

        self.connector = DropboxConnector(
            batch_size=config.get("batch_size", DEFAULT_BATCH_SIZE),
        )
        self.connector.load_credentials(credential.credential_json)
        self.batch_size = config.get("batch_size", DEFAULT_BATCH_SIZE)

    @property
    def _client(self):
        """Authenticated Dropbox SDK client, set up by Onyx's load_credentials."""
        if self.connector.dropbox_client is None:
            raise RuntimeError("Dropbox client not initialized — call load_credentials first")
        return self.connector.dropbox_client

    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        """List files from Dropbox using the SDK directly (metadata only)."""
        items: list[dict] = []
        max_size_bytes = settings.MAX_FILE_SIZE_MB * 1024 * 1024
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

        # Process all entries from this page
        for entry in result.entries:
            if not isinstance(entry, FileMetadata):
                continue

            # Incremental: skip files older than last successful run
            if start > 0 and entry.client_modified:
                modified = entry.client_modified
                if modified.tzinfo is None:
                    modified = modified.replace(tzinfo=timezone.utc)
                if modified.timestamp() < start:
                    continue

            # Skip oversized files
            if entry.size and entry.size > max_size_bytes:
                logger.warning(f"[DropboxCrawler] Skipping large file: {entry.name} ({entry.size / 1024 / 1024:.1f} MB)")
                skipped += 1
                continue

            items.append({
                "id": entry.id,
                "name": entry.name,
                "path_display": entry.path_display,
                "size": entry.size,
                "client_modified": (
                    entry.client_modified.isoformat()
                    if entry.client_modified
                    else None
                ),
            })

        logger.info(f"[DropboxCrawler] Page returned {len(items)} files, skipped {skipped} large files, has_more={result.has_more}")

        next_checkpoint = {
            "cursor": result.cursor,
            "has_more": result.has_more,
        }
        return items, next_checkpoint

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