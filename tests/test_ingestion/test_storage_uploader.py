from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from databricks.sdk import WorkspaceClient
from mypy_boto3_s3 import S3Client

from ingestion.storage_uploader import (
    ensure_bucket_exists,
    get_databricks_client,
    get_s3_client,
    upload_file_to_bucket,
    upload_file_to_volume,
)
from payment_recon.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


def test_get_s3_client():
    with patch("ingestion.storage_uploader.boto3.client") as mock_get_s3_client:
        get_s3_client()

        mock_get_s3_client.assert_called_once_with(
            "s3",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            endpoint_url=MINIO_ENDPOINT,
        )


def test_ensure_bucket_exists_when_not_found():
    mock_s3_client = MagicMock(spec=S3Client)
    mock_s3_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadBucket",
    )

    ensure_bucket_exists(bucket_name="test-bucket", s3_client=mock_s3_client)

    mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")
    mock_s3_client.create_bucket.assert_called_once_with(Bucket="test-bucket")


def test_ensure_bucket_exists_when_already_exist():
    mock_s3_client = MagicMock(spec=S3Client)

    ensure_bucket_exists(bucket_name="test-bucket", s3_client=mock_s3_client)

    mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")
    mock_s3_client.create_bucket.assert_not_called()


def test_ensure_bucket_exists_raises_on_other_client_error():
    mock_s3_client = MagicMock(spec=S3Client)
    mock_s3_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "403", "Message": "Forbidden"}},
        "HeadBucket",
    )

    with pytest.raises(ClientError):
        ensure_bucket_exists(bucket_name="test-bucket", s3_client=mock_s3_client)

    mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")
    mock_s3_client.create_bucket.assert_not_called()


def test_upload_file_success_with_custom_key(tmp_path: Path):
    file = tmp_path / "settlement.csv"
    file.write_text("sample_text")
    mock_s3_client = MagicMock(spec=S3Client)

    url = upload_file_to_bucket(
        file_path=file,
        bucket_name="payment-recon",
        object_key="bronze/settlements/settlement.csv",
        s3_client=mock_s3_client,
    )

    mock_s3_client.head_bucket.assert_called_once_with(Bucket="payment-recon")
    mock_s3_client.upload_file.assert_called_once_with(
        str(file),
        "payment-recon",
        "bronze/settlements/settlement.csv",
    )
    assert url == "s3a://payment-recon/bronze/settlements/settlement.csv"


def test_upload_file_strips_leading_slash_from_object_key(tmp_path: Path):
    file = tmp_path / "settlement.csv"
    file.write_text("sample_text")
    mock_s3_client = MagicMock(spec=S3Client)

    url = upload_file_to_bucket(
        file_path=file,
        bucket_name="payment-recon",
        object_key="/bronze/settlements/settlement.csv",
        s3_client=mock_s3_client,
    )

    mock_s3_client.upload_file.assert_called_once_with(
        str(file),
        "payment-recon",
        "bronze/settlements/settlement.csv",
    )
    assert url == "s3a://payment-recon/bronze/settlements/settlement.csv"


def test_upload_file_default_key_fallback(tmp_path: Path):
    file = tmp_path / "webhook_data.json"
    file.write_text("sample_text")
    mock_s3_client = MagicMock(spec=S3Client)

    url = upload_file_to_bucket(
        file_path=file,
        bucket_name="payment-recon",
        s3_client=mock_s3_client,
    )

    mock_s3_client.upload_file.assert_called_once_with(
        str(file),
        "payment-recon",
        "webhook_data.json",
    )
    assert url == "s3a://payment-recon/webhook_data.json"


def test_upload_file_missing_local_file_raises_error(tmp_path: Path):
    file = tmp_path / "webhook_data.json"
    mock_s3_client = MagicMock(spec=S3Client)

    with pytest.raises(FileNotFoundError):
        upload_file_to_bucket(
            file_path=file,
            bucket_name="payment-recon",
            object_key="data_sample/bronze/settlement.csv",
            s3_client=mock_s3_client,
        )

    mock_s3_client.head_bucket.assert_not_called()
    mock_s3_client.upload_file.assert_not_called()


def test_upload_file_initializes_client_if_none(tmp_path: Path):
    file = tmp_path / "webhook_data.json"
    file.write_text("sample_text")

    with patch("ingestion.storage_uploader.get_s3_client") as mock_get_s3_client:
        mock_client_instance = MagicMock(spec=S3Client)
        mock_get_s3_client.return_value = mock_client_instance

        upload_file_to_bucket(file_path=file, bucket_name="payment-recon")

        mock_get_s3_client.assert_called_once()


def test_get_databricks_client_missing_credentials_raises_error(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", False)
    monkeypatch.delenv("DATABRICKS_TOKEN", False)
    with pytest.raises(ValueError):
        get_databricks_client(host="", token=None)


def test_upload_file_to_volume_missing_file_raises_error(tmp_path: Path):
    path = tmp_path / "webhook.json"
    mock_dbx = MagicMock(spec=WorkspaceClient)

    with pytest.raises(FileNotFoundError):
        upload_file_to_volume(
            file_path=path,
            object_key="data_sample/bronze/webhook.json",
            dbx_client=mock_dbx,
        )

    mock_dbx.files.upload.assert_not_called()


def test_upload_file_to_volume_success_and_path_normalization(tmp_path: Path):
    path = tmp_path / "webhook.json"
    path.write_text('{"event": "test"}')
    mock_dbx = MagicMock(spec=WorkspaceClient)

    expected_target_path = (
        "/Volumes/workspace/payment_recon/bronze/webhooks/year=2026/webhook.json"
    )

    url = upload_file_to_volume(
        file_path=path,
        object_key="/bronze/webhooks/year=2026/webhook.json",
        dbx_client=mock_dbx,
    )

    mock_dbx.files.upload.assert_called_once_with(
        file_path=expected_target_path, contents=ANY, overwrite=True
    )
    assert url == expected_target_path


def test_upload_file_to_volume_default_key_fallback(tmp_path: Path):
    path = tmp_path / "webhook.json"
    path.write_text('{"event": "test"}')
    mock_dbx = MagicMock(spec=WorkspaceClient)

    url = upload_file_to_volume(
        file_path=path,
        dbx_client=mock_dbx,
    )

    mock_dbx.files.upload.assert_called_once_with(
        file_path="/Volumes/workspace/payment_recon/bronze/webhook.json",
        contents=ANY,
        overwrite=True,
    )
    assert url == "/Volumes/workspace/payment_recon/bronze/webhook.json"


def test_upload_file_to_volume_propagates_sdk_error(tmp_path: Path):
    path = tmp_path / "webhook.json"
    path.write_text('{"event": "test"}')
    mock_dbx = MagicMock(spec=WorkspaceClient)
    mock_dbx.files.upload.side_effect = RuntimeError("Databricks API 403 Forbidden")

    with pytest.raises(RuntimeError, match="Databricks API 403 Forbidden"):
        upload_file_to_volume(
            file_path=path,
            object_key="/bronze/webhooks/year=2026/webhook.json",
            dbx_client=mock_dbx,
        )


def test_upload_file_to_volume_initializes_client_if_none(tmp_path: Path):
    """Verify upload_file_to_volume instantiates Databricks client when dbx_client=None."""
    path = tmp_path / "wh.json"
    path.write_text("{}")
    mock_dbx = MagicMock(spec=WorkspaceClient)

    with patch(
        "ingestion.storage_uploader.get_databricks_client", return_value=mock_dbx
    ) as mock_get_dbx:
        url = upload_file_to_volume(file_path=path, dbx_client=None)
        mock_get_dbx.assert_called_once()
        mock_dbx.files.upload.assert_called_once()
        assert url.startswith("/Volumes/")
