from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from ingestion.storage_uploader import ensure_bucket_exists, get_s3_client, upload_file
from payment_recon.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY
import pytest


# get client
def test_get_s3_client():
    with patch("ingestion.storage_uploader.boto3.client") as mock_get_s3_client:
        get_s3_client()

        mock_get_s3_client.assert_called_once()

        mock_get_s3_client.assert_called_once_with(
            "s3",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            endpoint_url=MINIO_ENDPOINT,
        )


# ensure bucket
def test_ensure_bucket_exists_when_not_found():
    mock_s3_client: MagicMock | S3Client = MagicMock(spec=S3Client)

    mock_s3_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadBucket",
    )

    ensure_bucket_exists(bucket_name="test-bucket", s3_client=mock_s3_client)

    mock_s3_client.create_bucket.assert_called_once_with(Bucket="test-bucket")


def test_ensure_bucket_exists_when_already_exist():
    mock_s3_client: MagicMock | S3Client = MagicMock(spec=S3Client)

    ensure_bucket_exists(bucket_name="test-bucket", s3_client=mock_s3_client)

    mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")
    mock_s3_client.create_bucket.assert_not_called()


# upload file
def test_upload_file_success_with_custom_key(tmp_path):
    file = tmp_path / "settlement.csv"
    file.write_text("sample_text")

    mock_s3_client: MagicMock | S3Client = MagicMock(spec=S3Client)

    url = upload_file(
        file_path=file,
        bucket_name="payment-recon",
        object_key="data_sample/bronze/settlement.csv",
        s3_client=mock_s3_client,
    )

    mock_s3_client.upload_file.assert_called_once_with(
        file_path=file,
        bucket_name="payment-recon",
        object_key="data_sample/bronze/settlement.csv",
    )

    assert str(file)
    assert url == "s3a://payment-recon/bronze/settlements/settlement.csv"


def test_upload_file_default_key_fallback(tmp_path):
    file = tmp_path / "webhook_data.json"
    file.write_text("sample_text")

    mock_s3_client: MagicMock | S3Client = MagicMock(spec=S3Client)

    url = upload_file(
        file_path=file, bucket_name="payment-recon", s3_client=mock_s3_client
    )

    mock_s3_client.upload_file.assert_called_once_with(
        file_path=file, bucket_name="payment-recon"
    )
    assert url.endswith("webhook_data.json")


def test_upload_file_missing_local_file_raises_error(tmp_path):
    file = tmp_path / "webhook_data.json"

    mock_s3_client: MagicMock | S3Client = MagicMock(spec=S3Client)

    with pytest.raises(FileNotFoundError):
        upload_file(
            file_path=file,
            bucket_name="payment-recon",
            object_key="data_sample/bronze/settlement.csv",
            s3_client=mock_s3_client,
        )

    mock_s3_client.upload_file.assert_not_called()


def test_upload_file_initializes_client_if_none(tmp_path):
    file = tmp_path / "webhook_data.json"
    file.write_text("sample_text")

    with patch("ingestion.storage_uploader.get_s3_client") as mock_get_s3_client:
        upload_file(file_path=file, bucket_name="payment-recon")

    mock_get_s3_client.assert_called_once_with()
