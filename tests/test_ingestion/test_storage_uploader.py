from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
from ingestion.storage_uploader import get_s3_client, ensure_bucket_exists, upload_file
from payment_recon.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


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


def test_ensure_bucket_exists_when_not_found():
    mock_s3_client = MagicMock()

    mock_s3_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadBucket",
    )

    ensure_bucket_exists(bucket_name="test-bucket", s3_client=mock_s3_client)

    mock_s3_client.create_bucket.assert_called_once_with(Bucket="test-bucket")

def test_ensure_bucket_exists_when_found():
    