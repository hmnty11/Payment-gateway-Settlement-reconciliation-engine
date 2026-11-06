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
    Initialize a Boto3 S3 client configured for object storage.

    Retrieves connection parameters (endpoint URL, access key, and secret key)
    from `payment_recon.config` to connect to local MinIO or AWS S3.

    Returns
    -------
    S3Client
        Authenticated Boto3 S3 client instance.
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
    Verify existence of an S3 bucket and create it if it does not exist.

    Checks bucket presence via `head_bucket`. If the bucket is missing
    (HTTP 404 or 'NoSuchBucket'), creates it idempotently.

    Parameters
    ----------
    bucket_name : str
        Name of the target S3 bucket.
    s3_client : S3Client | None, default=None
        Active Boto3 S3 client. If None, initializes one via `get_s3_client()`.

    Raises
    ------
    botocore.exceptions.ClientError
        If an S3 error other than 404/NoSuchBucket is encountered (e.g., 403 Forbidden).
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
    Upload a local file to S3-compatible object storage (MinIO).

    Validates local file existence, ensures target bucket availability,
    and uploads the file content. Returns an S3A URI suitable for Spark ingest.

    Parameters
    ----------
    file_path : Path | str
        Path to the local file to upload.
    bucket_name : str
        Destination bucket name in object storage.
    object_key : str | None, default=None
        Destination object key. If None, defaults to the file's basename.
    s3_client : S3Client | None, default=None
        Active Boto3 S3 client. If None, initializes one via `get_s3_client()`.

    Returns
    -------
    str
        S3A formatted URI (e.g., `s3a://<bucket_name>/<object_key>`).

    Raises
    ------
    FileNotFoundError
        If `file_path` does not exist or is not a regular file.
    botocore.exceptions.ClientError
        If object upload fails due to permission or networking issues.
    """

    # change file_path format into path
    file_path = Path(file_path)

    # check whether filepath is valid or not
    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    # fallback if client not inputed and using get client to take the client
    if s3_client is None:
        s3_client = get_s3_client()

    ensure_bucket_exists(bucket_name=bucket_name, s3_client=s3_client)

    # fallback if object key is not inputted
    if not object_key:
        object_key = file_path.name.lstrip("/")
    else:
        object_key = object_key.lstrip("/")

    # upload file into bucket
    s3_client.upload_file(str(file_path), bucket_name, object_key)

    return f"s3a://{bucket_name}/{object_key}"
