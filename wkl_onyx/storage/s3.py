import boto3
from botocore.exceptions import ClientError
from storage.base import BlobStorageInterface


class S3BlobStorage(BlobStorageInterface):
    def __init__(
        self,
        bucket: str,
        aws_access_key: str,
        aws_secret_key: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
    ):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region,
            endpoint_url=endpoint_url,
        )

    def upload(self, key: str, data: bytes, metadata: dict | None = None) -> str:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            Metadata=metadata or {},
        )
        return f"s3://{self.bucket}/{key}"

    def download(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False