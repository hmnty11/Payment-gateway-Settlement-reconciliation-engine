from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from payment_recon.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


def get_s3_client() -> S3Client:
    """
    Initialize a Boto3 S3 client using configured credentials and endpoint.

    Returns
    -------
    S3Client
        Authenticated S3 client instance.
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
    Verify existence of an S3 bucket and create it if missing.

    Parameters
    ----------
    bucket_name : str
        Target bucket name to verify or create.
    s3_client : S3Client | None, default=None
        Boto3 S3 client. Initialized via `get_s3_client()` if None.

    Raises
    ------
    botocore.exceptions.ClientError
        If an S3 error other than 404 or NoSuchBucket occurs.
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


def upload_file_to_bucket(
    file_path: Path | str,
    bucket_name: str,
    object_key: str | None = None,
    s3_client: S3Client | None = None,
) -> str:
    """
    Upload a local file to S3/MinIO and return its S3A URI.

    Parameters
    ----------
    file_path : Path | str
        Path to the local source file.
    bucket_name : str
        Destination S3 bucket name.
    object_key : str | None, default=None
        Destination object key. Defaults to the source file name if None.
    s3_client : S3Client | None, default=None
        Boto3 S3 client. Initialized via `get_s3_client()` if None.

    Returns
    -------
    str
        S3A URI in the format `s3a://<bucket_name>/<object_key>`.

    Raises
    ------
    FileNotFoundError
        If `file_path` does not exist or is not a regular file.
    botocore.exceptions.ClientError
        If the upload operation fails.
    """

    file_path = Path(file_path)

    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    if s3_client is None:
        s3_client = get_s3_client()

    ensure_bucket_exists(bucket_name=bucket_name, s3_client=s3_client)

    if not object_key:
        object_key = file_path.name.lstrip("/")
    else:
        object_key = object_key.lstrip("/")

    s3_client.upload_file(str(file_path), bucket_name, object_key)

    return f"s3a://{bucket_name}/{object_key}"
