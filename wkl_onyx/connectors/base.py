from abc import ABC, abstractmethod
from core.config import settings


class BaseCrawler(ABC):
    def __init__(self, crawl_job, credential):
        self.crawl_job = crawl_job
        self.credential = credential
        config = crawl_job.config_json or {}
        self.batch_size: int = config.get("batch_size", settings.DEFAULT_BATCH_SIZE)
        self.max_file_size_bytes: int = (
            config.get("max_file_size_mb", settings.MAX_FILE_SIZE_MB) * 1024 * 1024
        )

    @abstractmethod
    def fetch_files(self, checkpoint: dict | None, start: float = 0) -> tuple[list[dict], dict]:
        """
        Returns (file_meta_list, next_checkpoint_json).
        start: unix timestamp - only return files modified after this time.
        """
        pass

    @abstractmethod
    def download(self, file_meta: dict) -> tuple[bytes, str, dict]:
        """Returns (content_bytes, filename, metadata_dict)"""
        pass