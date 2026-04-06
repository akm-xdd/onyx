import time

from connectors.base import BaseCrawler
from onyx.connectors.gmail.connector import GmailConnector, GmailCheckpoint
from onyx.connectors.models import Document
from onyx.connectors.interfaces import ConnectorFailure
import json
DEFAULT_BATCH_SIZE = 100


class GmailCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        config = crawl_job.config_json or {}

        self.connector = GmailConnector(
            batch_size=config.get("batch_size", DEFAULT_BATCH_SIZE),
        )
        cred_json = dict(credential.credential_json)
        if isinstance(cred_json.get("google_tokens"), dict):
            tokens = dict(cred_json["google_tokens"])
            tokens.setdefault("expiry", None)
            tokens.setdefault("universe_domain", "googleapis.com")
            tokens.setdefault("account", "")
            cred_json["google_tokens"] = json.dumps(tokens)
        self.connector.load_credentials(cred_json)
        self.batch_size = config.get("batch_size", DEFAULT_BATCH_SIZE)


    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        if checkpoint:
            ckpt = GmailCheckpoint.model_validate(checkpoint)
            if not ckpt.has_more:
                ckpt = self.connector.build_dummy_checkpoint()
        else:
            ckpt = self.connector.build_dummy_checkpoint()

        end = time.time()
        items = []

        print(f"[GmailCrawler] Starting fetch, checkpoint has_more={ckpt.has_more}, users={len(ckpt.user_emails)}")

        gen = self.connector.load_from_checkpoint(
            start=start,
            end=end,
            checkpoint=ckpt,
        )

        try:
            while True:
                result = next(gen)
                
                if isinstance(result, ConnectorFailure):
                    print(f"[GmailCrawler] Failure: {result.failure_message}")
                    continue
                if not isinstance(result, Document):
                    continue

                items.append({
                    "id": result.id,
                    "name": result.semantic_identifier or result.id,
                    "content": self._serialize_document(result),
                    "doc_updated_at": result.doc_updated_at.isoformat() if result.doc_updated_at else None,
                    "metadata": result.metadata,
                    "source": str(result.source),
                    "primary_owners": [
                        {"email": o.email, "first_name": o.first_name, "last_name": o.last_name}
                        for o in (result.primary_owners or [])
                    ],
                    "secondary_owners": [
                        {"email": o.email, "first_name": o.first_name, "last_name": o.last_name}
                        for o in (result.secondary_owners or [])
                    ],
                })
        except StopIteration as e:
            if e.value is not None:
                ckpt = e.value

        next_checkpoint = ckpt.model_dump(mode="json")
        print(f"[GmailCrawler] Fetched {len(items)} threads, has_more={ckpt.has_more}")
        return items, next_checkpoint

    def _serialize_document(self, doc: Document) -> str:
        parts = []
        for section in doc.sections:
            if hasattr(section, "text") and section.text:
                parts.append(section.text)
        return "\n\n---\n\n".join(parts)

    def download(self, file_meta: dict) -> tuple[bytes, str, dict]:
        content = file_meta.get("content", "")
        name = file_meta.get("name", "unknown")
        
        # Use ID prefix to avoid collisions
        filename = f"{file_meta['id']}_{name}.md"
        for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '#', '\n', '\r']:
            filename = filename.replace(char, "_")
        if len(filename) > 200:
            filename = filename[:190] + ".md"

        metadata = {
            "source": file_meta.get("source", ""),
            "doc_updated_at": file_meta.get("doc_updated_at"),
            "primary_owners": file_meta.get("primary_owners", []),
            "secondary_owners": file_meta.get("secondary_owners", []),
        }
        if file_meta.get("metadata"):
            metadata.update(file_meta["metadata"])

        return content.encode("utf-8"), filename, metadata