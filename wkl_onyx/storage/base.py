from abc import ABC, abstractmethod


class BlobStorageInterface(ABC):

    @abstractmethod
    def upload(self, key: str, data: bytes, metadata: dict | None = None) -> str:
        raise NotImplementedError

    @abstractmethod
    def download(self, key: str) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    def count(self, prefix: str) -> int:
        raise NotImplementedError   