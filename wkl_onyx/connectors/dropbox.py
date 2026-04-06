import time

from connectors.base import BaseCrawler
from onyx.connectors.dropbox.connector import DropboxConnector
from onyx.connectors.models import Document

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

    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        items = []

        if getattr(self, "_doc_generator", None) is None:
            if start > 0:
                self._doc_generator = self.connector.poll_source(start=start, end=time.time())
            else:
                self._doc_generator = self.connector.load_from_state()

        for batch in self._doc_generator:
            for doc_or_node in batch:
                if not isinstance(doc_or_node, Document):
                    continue

                items.append({
                    "id": doc_or_node.id,
                    "name": doc_or_node.semantic_identifier or doc_or_node.id,
                    "content": self._serialize_document(doc_or_node),
                    "doc_updated_at": doc_or_node.doc_updated_at.isoformat() if doc_or_node.doc_updated_at else None,
                    "metadata": doc_or_node.metadata,
                    "source": str(doc_or_node.source),
                })

                if len(items) >= self.batch_size:
                    return items, {"has_more": True}

        return items, {"has_more": False}


    def _serialize_document(self, doc: Document) -> str:
        parts = []
        for section in doc.sections:
            if hasattr(section, 'text') and section.text:
                parts.append(section.text)
        return "\n".join(parts)

    def download(self, file_meta: dict) -> tuple[bytes, str, dict]:
        content = file_meta.get("content", "")
        name = file_meta.get("name", "unknown")
        filename = name
        for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '#']:
            filename = filename.replace(char, "_")

        metadata = {
            "source": file_meta.get("source", ""),
            "doc_updated_at": file_meta.get("doc_updated_at"),
        }
        if file_meta.get("metadata"):
            metadata.update(file_meta["metadata"])
            
            # Translate the Dropbox path_display into the standard folder_path list
            path_display = metadata.get("path")
            if path_display and isinstance(path_display, str):
                parts = [p for p in path_display.split('/') if p]
                if len(parts) > 1:
                    metadata["folder_path"] = parts[:-1]
                else:
                    metadata["folder_path"] = []

        return content.encode("latin-1"), filename, metadata