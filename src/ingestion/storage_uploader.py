from pathlib import Path

import boto3

from payment_recon.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


def get_s3_client() -> object:
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


def ensure_bucket_exists(bucket_name: str, s3_client: str | None = None) -> None:
    """
    ensure bucket exist using head_bucket method,
    if dont exist it will make a new one using create_bucket method
    """


def upload_file(
    file_path: Path | str,
    bucket_name: str,
    object_key: str,
    s3_client: str | None = None,
) -> str:
    """
    ensure file exist locally,
    call ensure_bucket function,
    transfer file into minio using upload_file method from boto3
    """
