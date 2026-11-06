from spark_jobs.gen_and_ingestion_runner import (
    run_daily_ingestion,
    run_date_range_ingestion,
)
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import date
from mypy_boto3_s3 import S3Client
import pytest


def test_date_range_invalid_raises_error():

    mock_s3_client = MagicMock(spec=S3Client)
    with pytest.raises(ValueError):
        run_date_range_ingestion(
            start_date=2026 - 12 - 23, end_date=2025 - 12 - 28, s3_client=mock_s3_client
        )


def test_single_day_default_fallback():

    mock_s3_client = MagicMock(spec=S3Client)
    run_date_range_ingestion(start_date=2025 - 3 - 12, s3_client=mock_s3_client)

    mock_s3_client.run_daily_ingestion.assert_called_once_with(
        "2025-3-12", 100, "data_sample/", "payment-recon"
    )

    # note: have to be the same date with the same date
    mock_s3_client.generate_batch_webhook.assert_called()
    mock_s3_client.generate_batch_settlement.assert_called()
    mock_s3_client.upload_file_to_bucket.assert_called()

def test_date_range_multi_day_execution():
    