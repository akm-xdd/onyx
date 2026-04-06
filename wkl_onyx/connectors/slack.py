import time

from slack_sdk import WebClient
from slack_sdk.http_retry import ConnectionErrorRetryHandler
from slack_sdk.http_retry.builtin_interval_calculators import FixedValueRetryIntervalCalculator

from connectors.base import BaseCrawler
from onyx.connectors.models import Document
from onyx.connectors.interfaces import ConnectorFailure
from onyx.connectors.slack.connector import SlackConnector, SlackCheckpoint
from onyx.connectors.slack.utils import SlackTextCleaner


class SlackCrawler(BaseCrawler):
    def __init__(self, crawl_job, credential):
        super().__init__(crawl_job, credential)
        config = crawl_job.config_json or {}

        bot_token = credential.credential_json.get("slack_bot_token")
        if not bot_token:
            raise ValueError("slack_bot_token missing from credential")

        self.connector = SlackConnector(
            channels=config.get("channels") or None,
            channel_regex_enabled=config.get("channel_regex_enabled", False),
            include_bot_messages=config.get("include_bot_messages", False),
            use_redis=False,
        )

        retry_handler = ConnectionErrorRetryHandler(
            max_retry_count=7,
            interval_calculator=FixedValueRetryIntervalCalculator(),
        )
        self.connector.client = WebClient(token=bot_token, retry_handlers=[retry_handler])
        self.connector.fast_client = WebClient(token=bot_token, timeout=1)
        self.connector.text_cleaner = SlackTextCleaner(client=self.connector.client)

        try:
            auth = self.connector.client.auth_test()
            self.connector._workspace_url = auth.get("url")
        except Exception:
            self.connector._workspace_url = None

    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        if checkpoint:
            ckpt = SlackCheckpoint.model_validate(checkpoint)
            if not ckpt.has_more:
                ckpt = self.connector.build_dummy_checkpoint()
        else:
            ckpt = self.connector.build_dummy_checkpoint()

        end = time.time()
        items = []

        gen = self.connector.load_from_checkpoint(
            start=start,
            end=end,
            checkpoint=ckpt,
        )

        try:
            while True:
                result = next(gen)

                if isinstance(result, ConnectorFailure):
                    print(f"[SlackCrawler] Failure: {result.failure_message}")
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
                })
        except StopIteration as e:
            if e.value is not None:
                ckpt = e.value

        next_checkpoint = ckpt.model_dump(mode="json")
        print(f"[SlackCrawler] Fetched {len(items)} threads, has_more={ckpt.has_more}")
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

        filename = f"{file_meta['id']}_{name}.md"
        for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '#', '\n', '\r']:
            filename = filename.replace(char, "_")
        if len(filename) > 200:
            filename = filename[:190] + ".md"

        metadata = {
            "source": file_meta.get("source", ""),
            "doc_updated_at": file_meta.get("doc_updated_at"),
        }
        if file_meta.get("metadata"):
            metadata.update(file_meta["metadata"])

        return content.encode("utf-8"), filename, metadata