import time
from datetime import datetime, timezone
from typing import Any

from connectors.base import BaseCrawler
from onyx.connectors.github.connector import GithubConnector
from core.config import settings

DEFAULT_BATCH_SIZE = 100


class GithubCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        config = crawl_job.config_json or {}

        self.connector = GithubConnector(
            repo_owner=config.get("repo_owner", ""),
            repositories=config.get("repositories") or None,
            state_filter=config.get("state_filter", "all"),
            include_prs=config.get("include_prs", True),
            include_issues=config.get("include_issues", True),
        )

        cred_json = dict(credential.credential_json)
        self.connector.load_credentials(cred_json)
        self.batch_size = config.get("batch_size", DEFAULT_BATCH_SIZE)

    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        from onyx.connectors.github.connector import GithubConnectorCheckpoint
        from onyx.connectors.models import Document
        from onyx.connectors.interfaces import ConnectorFailure

        if checkpoint:
            ckpt = GithubConnectorCheckpoint.model_validate(checkpoint)
            if not ckpt.has_more:
                ckpt = self.connector.build_dummy_checkpoint()
        else:
            ckpt = self.connector.build_dummy_checkpoint()

        start_ts = start if start > 0 else 0
        end_ts = time.time()
        items = []
        max_iterations = 20  # safety net

        for i in range(max_iterations):
            if not ckpt.has_more or len(items) >= self.batch_size:
                break

            print(f"[github:fetch] Iteration {i} - stage={ckpt.stage}, has_more={ckpt.has_more}, cached_repo_ids={ckpt.cached_repo_ids}")

            gen = self.connector._fetch_from_github(
                checkpoint=ckpt,
                start=datetime.fromtimestamp(start_ts, tz=timezone.utc) if start_ts > 0 else None,
                end=datetime.fromtimestamp(end_ts, tz=timezone.utc),
                include_permissions=False,
            )

            # Manually drive the generator to capture return value
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
                        print(f"[github:fetch] Failure: {result.failure_message}")
                except StopIteration as e:
                    returned_ckpt = e.value
                    break

            if returned_ckpt is not None:
                print(f"[github:fetch] Got checkpoint back - has_more={returned_ckpt.has_more}, stage={returned_ckpt.stage}")
                ckpt = returned_ckpt
            else:
                break

        print(f"[github:fetch] Fetched {len(items)} items after {i+1} iterations")
        next_checkpoint = ckpt.model_dump(mode="json") if hasattr(ckpt, 'model_dump') else {}
        return items, next_checkpoint

    def _serialize_document(self, doc) -> str:
        """Extract text content from document sections."""
        parts = []
        for section in doc.sections:
            if hasattr(section, 'text') and section.text:
                parts.append(section.text)
        return "\n".join(parts)

    def download(self, file_meta: dict) -> tuple[bytes, str, dict]:
        content = file_meta.get("content", "")
        name = file_meta.get("name", "unknown")
        filename = f"{name}.md" if not name.endswith(".md") else name
        # Sanitize all characters invalid in S3 keys
        for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '#']:
            filename = filename.replace(char, "_")
        
        metadata = {
            "source": file_meta.get("source", ""),
            "doc_updated_at": file_meta.get("doc_updated_at"),
        }
        if file_meta.get("metadata"):
            metadata.update(file_meta["metadata"])

        return content.encode("utf-8"), filename, metadata
