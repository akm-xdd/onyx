from .base import BlobStorageInterface


class AzureBlobStorage(BlobStorageInterface):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Azure storage not yet implemented")

    def upload(self, key: str, data: bytes, metadata: dict | None = None) -> str:
        raise NotImplementedError

    def download(self, key: str) -> bytes:
        raise NotImplementedError

    def delete(self, key: str) -> None:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError