from abc import ABC, abstractmethod


class BaseCrawler(ABC):
    def __init__(self, crawl_job, credential):
        self.crawl_job = crawl_job
        self.credential = credential

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