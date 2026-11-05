from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from payment_recon.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


def get_s3_client() -> S3Client:
    """
    initialize s3 object client from boto3 library using
    configuration from payment_recon.config
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
    )

    return s3


def ensure_bucket_exists(bucket_name: str, s3_client: S3Client | None = None) -> None:
    """
    ensure bucket exist using head_bucket method,
    if dont exist it will make a new one using create_bucket method
    """
    if s3_client is None:
        s3_client = get_s3_client()

    try:
        s3_client.head_bucket(Bucket=bucket_name)

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")

        if error_code == "404" or error_code == "NoSuchBucket":
            s3_client.create_bucket(Bucket=bucket_name)

        else:
            raise


def upload_file(
    file_path: Path | str,
    bucket_name: str,
    object_key: str | None = None,
    s3_client: S3Client | None = None,
) -> str:
    """
    ensure file exist locally,
    call ensure_bucket function,
    transfer file into minio using upload_file method from boto3
    """
