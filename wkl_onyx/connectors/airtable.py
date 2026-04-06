# connectors/airtable.py
from connectors.base import BaseCrawler
from onyx.connectors.airtable.airtable_connector import AirtableConnector
from onyx.connectors.models import Document


class AirtableCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        config = crawl_job.config_json or {}

        self.connector = AirtableConnector(
            base_id=config.get("base_id", ""),
            table_name_or_id=config.get("table_name_or_id", ""),
            airtable_url=config.get("airtable_url", ""),
            view_id=config.get("view_id"),
            treat_all_non_attachment_fields_as_metadata=config.get("treat_all_non_attachment_fields_as_metadata", False),
        )
        self.connector.load_credentials(credential.credential_json)

    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        if start != 0:
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
        file_id = file_meta.get("id", name)
        safe_id = file_id.replace("airtable__", "")
        filename = f"{safe_id}.md"
        for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '#']:
            filename = filename.replace(char, "_")

        # Extract table name from semantic_identifier for folder path
        table_name = name.split(":")[0].strip() if ":" in name else "default"

        metadata = {
            "source": file_meta.get("source", ""),
            "doc_updated_at": file_meta.get("doc_updated_at"),
            "folder_path": [table_name],
        }
        if file_meta.get("metadata"):
            metadata.update(file_meta["metadata"])

        return content.encode("utf-8"), filename, metadata