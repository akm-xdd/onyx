import time

from connectors.base import BaseCrawler
from onyx.connectors.notion.connector import NotionConnector
from onyx.connectors.models import Document

DEFAULT_BATCH_SIZE = 100


class NotionCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        config = crawl_job.config_json or {}

        self.connector = NotionConnector(
            root_page_id=config.get("root_page_id"),
            recursive_index_enabled=config.get("recursive_index_enabled", True),
        )
        self.connector.load_credentials(credential.credential_json)
        self.batch_size = config.get("batch_size", DEFAULT_BATCH_SIZE)

    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        items = []

        if start > 0:
            # Incremental
            end = time.time()
            doc_batches = self.connector.poll_source(start=start, end=end)
        else:
            # Full crawl
            doc_batches = self.connector.load_from_state()

        for batch in doc_batches:
            for doc_or_node in batch:
                if not isinstance(doc_or_node, Document):
                    continue  # skip HierarchyNodes

                items.append({
                    "id": doc_or_node.id,
                    "name": doc_or_node.semantic_identifier or doc_or_node.id,
                    "content": self._serialize_document(doc_or_node),
                    "doc_updated_at": doc_or_node.doc_updated_at.isoformat() if doc_or_node.doc_updated_at else None,
                    "metadata": doc_or_node.metadata,
                    "source": str(doc_or_node.source),
                })

                if len(items) >= self.batch_size:
                    # Notion connector doesn't use checkpoints, return all at once
                    return items, {"has_more": False}

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
        filename = f"{name}.md" if not name.endswith(".md") else name
        for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '#']:
            filename = filename.replace(char, "_")

        metadata = {
            "source": file_meta.get("source", ""),
            "doc_updated_at": file_meta.get("doc_updated_at"),
        }
        if file_meta.get("metadata"):
            metadata.update(file_meta["metadata"])

        return content.encode("utf-8"), filename, metadata