# storage/__init__.py
from core.config import settings
from .base import BlobStorageInterface
from .s3 import S3BlobStorage

_instance: BlobStorageInterface | None = None


def get_storage() -> BlobStorageInterface:
    global _instance
    if _instance is None:
        if settings.STORAGE_BACKEND in ("s3", "minio"):
            _instance = S3BlobStorage(
                bucket=settings.MINIO_BUCKET,
                aws_access_key=settings.MINIO_ACCESS_KEY,
                aws_secret_key=settings.MINIO_SECRET_KEY,
                endpoint_url=settings.MINIO_ENDPOINT if settings.STORAGE_BACKEND == "minio" else None,
            )
        elif settings.STORAGE_BACKEND == "azure":
            from .azure import AzureBlobStorage
            _instance = AzureBlobStorage()
        else:
            raise ValueError(f"Unknown storage backend: {settings.STORAGE_BACKEND}")
    return _instance