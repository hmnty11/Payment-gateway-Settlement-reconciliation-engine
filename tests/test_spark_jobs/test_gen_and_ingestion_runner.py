from spark_jobs.gen_and_ingestion_runner import (
    run_daily_ingestion,
    run_date_range_ingestion,
)
from unittest.mock import call
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import date
from mypy_boto3_s3 import S3Client
import pytest


def test_date_range_invalid_raises_error():

    mock_s3_client = MagicMock(spec=S3Client)
    with pytest.raises(ValueError):
        run_date_range_ingestion(
            start_date=date(2026,12,23), end_date=date(2025,12,28), s3_client=mock_s3_client
        )


def test_single_day_default_fallback():

    mock_s3_client = MagicMock(spec=S3Client)

    with patch("spark_jobs.gen_and_ingestion_runner.run_daily_ingestion") as mock_daily:
        run_date_range_ingestion(start_date=date(2026, 11, 1), s3_client=mock_s3_client)

        mock_daily.assert_called_once()

    # note: have to be the same date with the same date
    mock_s3_client.generate_batch_webhook.assert_called()
    mock_s3_client.generate_batch_settlement.assert_called()
    mock_s3_client.upload_file_to_bucket.assert_called()


def test_date_range_multi_day_execution():

    mock_s3_client = MagicMock(spec=S3Client)
    

    with patch("spark_jobs.gen_and_ingestion_runner.run_daily_ingestion") as mock_daily:
        run_date_range_ingestion(start_date=date(2026, 11, 1), s3_client=mock_s3_client)
        assert mock_daily.call_count == 3
        mock_daily.assert_has_calls([
            call(target_date=date(2026, 11, 1), s3_client= mock_s3_client),
            call(target_date=date(2026, 11, 2), s3_client= mock_s3_client),
            call(target_date=date(2026, 11, 3), s3_client= mock_s3_client),
        ], any_order=False)


def test_pending_buffer_propagates_to_next_day():
