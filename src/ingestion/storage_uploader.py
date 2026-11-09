from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError
from databricks.sdk import WorkspaceClient

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from payment_recon.config import (
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)


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


# databricks
def get_databricks_client(
    host: str | None = None, token: str | None = None
) -> WorkspaceClient:

    if host is None:
        host = DATABRICKS_HOST

    if token is None:
        token = DATABRICKS_TOKEN

    if not host or not token:
        raise ValueError("please input host or token for databricks in .env file")

    else:
        host = host.rstrip("/")

    w = WorkspaceClient(host=host, token=token)
    return w


def upload_file_to_volume(
    file_path: Path | str,
    object_key: str,
    volume_base_path: str,
    dbx_client: WorkspaceClient | None = None,
) -> str:
    file_path = Path(file_path)

    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    if dbx_client is None:
        dbx_client = get_databricks_client(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

    volume_base_path = volume_base_path.rstrip("/")
    object_key = object_key.lstrip("/").removeprefix("bronze/")

    target_volume_path = f"{volume_base_path}/{object_key}"  # merge base path and object key to produce target volume path

    with open(file_path, "rb") as f:
        dbx_client.files.upload(
            file_path=target_volume_path, contents=f, overwrite=True
        )

    return target_volume_path
