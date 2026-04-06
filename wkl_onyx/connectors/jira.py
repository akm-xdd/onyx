# connectors/jira.py
import time
from typing import Any

from connectors.base import BaseCrawler
from onyx.connectors.jira.connector import JiraConnector, JiraConnectorCheckpoint
from onyx.connectors.models import Document
from onyx.connectors.interfaces import ConnectorFailure
from datetime import datetime, timezone

DEFAULT_BATCH_SIZE = 100


class JiraCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        config = crawl_job.config_json or {}

        self.connector = JiraConnector(
            jira_base_url=config.get("jira_base_url", ""),
            project_key=config.get("project_key"),
            comment_email_blacklist=config.get("comment_email_blacklist", []),
            labels_to_skip=config.get("labels_to_skip", []),
            jql_query=config.get("jql_query"),
        )

        cred_json = dict(credential.credential_json)
        self.connector.load_credentials(cred_json)
        self.batch_size = config.get("batch_size", DEFAULT_BATCH_SIZE)


    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        if checkpoint:
            ckpt = JiraConnectorCheckpoint.model_validate(checkpoint)
            if not ckpt.has_more:
                ckpt = self.connector.build_dummy_checkpoint()
        else:
            ckpt = self.connector.build_dummy_checkpoint()

        start_ts = start if start > 0 else 946684800  # 2000-01-01
        end_ts = time.time() + 86400  # +24h timezone buffer

        utc_offset = datetime.now(timezone.utc).astimezone().utcoffset().total_seconds()
        start_ts += utc_offset
        end_ts += utc_offset

        items = []

        gen = self.connector.load_from_checkpoint(
            start=start_ts,
            end=end_ts,
            checkpoint=ckpt,
        )

        returned_ckpt = None
        while True:
            try:
                result = next(gen)
                if isinstance(result, Document):
                    items.append({
                        "id": result.id,
                        "name": result.semantic_identifier or result.id,
                        "content": self._serialize_document(result),
                        "doc_updated_at": result.doc_updated_at.isoformat() if result.doc_updated_at else None,
                        "metadata": result.metadata,
                        "source": str(result.source),
                    })
                elif isinstance(result, ConnectorFailure):
                    print(f"[jira:fetch] Failure: {result.failure_message}")
            except StopIteration as e:
                returned_ckpt = e.value
                break

        next_checkpoint = returned_ckpt.model_dump(mode="json") if returned_ckpt and hasattr(returned_ckpt, 'model_dump') else {"has_more": False}
        return items, next_checkpoint


    def _serialize_document(self, doc) -> str:
        parts = []

        # Add metadata header
        if doc.metadata:
            for key, value in doc.metadata.items():
                if value:
                    parts.append(f"{key}: {value}")
            parts.append("")  # blank line separator

        # Add content sections
        for section in doc.sections:
            if hasattr(section, 'text') and section.text:
                parts.append(section.text)

        return "\n".join(parts)


    def download(self, file_meta: dict) -> tuple[bytes, str, dict]:
        content = file_meta.get("content", "")
        name = file_meta.get("name", "unknown")
        file_id = file_meta.get("id", name)
        safe_id = file_id.rsplit("/", 1)[-1] if "/" in file_id else file_id
        filename = f"{safe_id}.md"
        for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '#']:
            filename = filename.replace(char, "_")

        # Extract project key from issue key (e.g., "SCRUM-1" -> "SCRUM")
        project_key = safe_id.rsplit("-", 1)[0] if "-" in safe_id else "default"

        metadata = {
            "source": file_meta.get("source", ""),
            "doc_updated_at": file_meta.get("doc_updated_at"),
            "folder_path": [project_key],
        }
        if file_meta.get("metadata"):
            metadata.update(file_meta["metadata"])

        return content.encode("utf-8"), filename, metadata