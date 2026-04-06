# connectors/web.py
import time
from connectors.base import BaseCrawler
from onyx.connectors.web.connector import WebConnector
from onyx.connectors.models import Document

DEFAULT_BATCH_SIZE = 100


class WebCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        config = crawl_job.config_json or {}

        self.connector = WebConnector(
            base_url=config.get("base_url", ""),
            web_connector_type=config.get("web_connector_type", "recursive"),
            mintlify_cleanup=config.get("mintlify_cleanup", True),
            scroll_before_scraping=config.get("scroll_before_scraping", False),
        )
        self.connector.load_credentials({})

    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:

        if start !=0:
            return [], {"has_more": False}

        items = []

        for batch in self.connector.load_from_state():
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

        return items, {"has_more": False}

    def _serialize_document(self, doc) -> str:
        parts = []
        if doc.metadata:
            for key, value in doc.metadata.items():
                if value:
                    parts.append(f"{key}: {value}")
            parts.append("")
        for section in doc.sections:
            if hasattr(section, 'text') and section.text:
                parts.append(section.text)
        return "\n".join(parts)

    def download(self, file_meta: dict) -> tuple[bytes, str, dict]:
        content = file_meta.get("content", "")
        name = file_meta.get("name", "unknown")
        # Use URL path as filename
        url = file_meta.get("id", name)
        from urllib.parse import urlparse
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_") or parsed.netloc
        filename = f"{path}.md"
        for char in ['\\', ':', '*', '?', '"', '<', '>', '|', '#']:
            filename = filename.replace(char, "_")

        # Use domain as folder path
        domain = parsed.netloc.replace(":", "_")
        metadata = {
            "source": file_meta.get("source", ""),
            "doc_updated_at": file_meta.get("doc_updated_at"),
            "folder_path": [domain],
        }
        if file_meta.get("metadata"):
            metadata.update(file_meta["metadata"])

        return content.encode("utf-8"), filename, metadata