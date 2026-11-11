from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from mypy_boto3_s3 import S3Client

from spark_jobs.gen_and_ingestion_runner import (
    run_daily_ingestion,
    run_date_range_ingestion,
    split_webhooks_for_settlement,
)


def test_split_webhooks_filters_out_anomalies():
    sample_webhooks = [
        {
            "event": {"status": "success"},
            "transaction": {"transaction_id": "trx_valid_1", "amount": 50000.0},
        },
        {
            "event": {"status": "success"},
            "transaction": {"transaction_id": None, "amount": 50000.0},
        },
        {
            "event": {"status": "success"},
            "transaction": {"transaction_id": "trx_invalid_2", "amount": -1000.0},
        },
        {
            "event": {"status": "failed"},
            "transaction": {"transaction_id": "trx_failed_3", "amount": 75000.0},
        },
    ]

    settle_today, pending = split_webhooks_for_settlement(
        sample_webhooks, late_rate=0.0
    )

    assert len(settle_today) == 1
    assert len(pending) == 0
    assert settle_today[0]["transaction"]["transaction_id"] == "trx_valid_1"


def test_split_webhooks_buffer_split_probability():
    sample_webhooks = [
        {
            "event": {"status": "success"},
            "transaction": {"transaction_id": f"trx_{i}", "amount": 10000.0},
        }
        for i in range(10)
    ]
    settle_today, pending = split_webhooks_for_settlement(
        sample_webhooks, late_rate=1.0
    )
    assert len(settle_today) == 0
    assert len(pending) == 10


def test_run_daily_ingestion_creates_hive_partitions_and_uploads(
    tmp_path: Path, monkeypatch
):
    mock_s3_client = MagicMock(spec=S3Client)
    test_date = date(2026, 11, 1)
    monkeypatch.setattr("spark_jobs.gen_and_ingestion_runner.ENV_MODE", "local")
    monkeypatch.setattr(
        "spark_jobs.gen_and_ingestion_runner.BRONZE_WEBHOOKS_PATH",
        "s3a://payment-recon/bronze/webhooks",
    )
    monkeypatch.setattr(
        "spark_jobs.gen_and_ingestion_runner.BRONZE_SETTLEMENTS_PATH",
        "s3a://payment-recon/bronze/settlements",
    )

    with patch(
        "spark_jobs.gen_and_ingestion_runner.upload_file_to_bucket"
    ) as mock_upload:
        mock_upload.side_effect = [
            "s3a://payment-recon/bronze/webhooks/year=2026/month=11/day=01/wh.json",
            "s3a://payment-recon/bronze/settlements/year=2026/month=11/day=01/st.csv",
        ]

        result = run_daily_ingestion(
            target_date=test_date,
            num_records=5,
            output_dir=tmp_path,
            bucket_name="payment-recon",
            s3_client=mock_s3_client,
        )

        assert result["target_date"] == test_date
        assert result["webhook_s3_uri"].startswith("s3a://")
        assert result["settlement_s3_uri"].startswith("s3a://")
        assert isinstance(result["pending_buffer"], list)

        assert mock_upload.call_count == 2

        webhook_key = mock_upload.call_args_list[0].args[2]
        settlement_key = mock_upload.call_args_list[1].args[2]

        assert webhook_key.startswith("bronze/webhooks/year=2026/month=11/day=01/")
        assert settlement_key.startswith(
            "bronze/settlements/year=2026/month=11/day=01/"
        )


def test_run_daily_ingestion_databricks_uploads_to_volume(tmp_path: Path, monkeypatch):
    mock_dbx_client = MagicMock()
    test_date = date(2026, 11, 1)
    monkeypatch.setattr("spark_jobs.gen_and_ingestion_runner.ENV_MODE", "databricks")
    monkeypatch.setattr(
        "spark_jobs.gen_and_ingestion_runner.BRONZE_WEBHOOKS_PATH",
        "/Volumes/workspace/payment_recon/bronze/webhooks",
    )
    monkeypatch.setattr(
        "spark_jobs.gen_and_ingestion_runner.BRONZE_SETTLEMENTS_PATH",
        "/Volumes/workspace/payment_recon/bronze/settlements",
    )

    with patch(
        "spark_jobs.gen_and_ingestion_runner.upload_file_to_volume"
    ) as mock_upload:
        mock_upload.side_effect = [
            "/Volumes/workspace/payment_recon/bronze/webhooks/year=2026/month=11/day=01/wh.json",
            "/Volumes/workspace/payment_recon/bronze/settlements/year=2026/month=11/day=01/st.csv",
        ]

        result = run_daily_ingestion(
            target_date=test_date,
            num_records=5,
            output_dir=tmp_path,
            dbx_client=mock_dbx_client,
        )

        assert result["target_date"] == test_date
        assert result["webhook_s3_uri"].startswith("/Volumes/")
        assert result["settlement_s3_uri"].startswith("/Volumes/")
        assert mock_upload.call_count == 2

        webhook_key = mock_upload.call_args_list[0].args[1]
        settlement_key = mock_upload.call_args_list[1].args[1]

        assert webhook_key.startswith("webhooks/year=2026/month=11/day=01/")
        assert settlement_key.startswith("settlements/year=2026/month=11/day=01/")


def test_date_range_invalid_raises_error():
    mock_s3_client = MagicMock(spec=S3Client)
    with pytest.raises(ValueError, match="start_date cannot be greater than end_date"):
        run_date_range_ingestion(
            start_date=date(2026, 12, 23),
            end_date=date(2025, 12, 28),
            s3_client=mock_s3_client,
        )


def test_single_day_default_fallback():
    mock_s3_client = MagicMock(spec=S3Client)

    with patch("spark_jobs.gen_and_ingestion_runner.run_daily_ingestion") as mock_daily:
        mock_daily.return_value = {"pending_buffer": []}

        run_date_range_ingestion(start_date=date(2026, 11, 1), s3_client=mock_s3_client)

        mock_daily.assert_called_once()
        assert mock_daily.call_args.kwargs["target_date"] == date(2026, 11, 1)


def test_date_range_multi_day_execution():
    mock_s3_client = MagicMock(spec=S3Client)

    with patch("spark_jobs.gen_and_ingestion_runner.run_daily_ingestion") as mock_daily:
        mock_daily.return_value = {"pending_buffer": []}

        run_date_range_ingestion(
            start_date=date(2026, 11, 1),
            end_date=date(2026, 11, 3),
            s3_client=mock_s3_client,
        )

        assert mock_daily.call_count == 3
        invoked_dates = [
            call_item.kwargs["target_date"] for call_item in mock_daily.call_args_list
        ]
        assert invoked_dates == [
            date(2026, 11, 1),
            date(2026, 11, 2),
            date(2026, 11, 3),
        ]


def test_date_range_databricks_mode_passes_dbx_client(monkeypatch):
    mock_dbx_client = MagicMock()
    monkeypatch.setattr("spark_jobs.gen_and_ingestion_runner.ENV_MODE", "databricks")

    with patch("spark_jobs.gen_and_ingestion_runner.run_daily_ingestion") as mock_daily:
        mock_daily.return_value = {"pending_buffer": []}

        run_date_range_ingestion(
            start_date=date(2026, 11, 1),
            end_date=date(2026, 11, 2),
            dbx_client=mock_dbx_client,
        )

        assert mock_daily.call_count == 2
        for call_item in mock_daily.call_args_list:
            assert call_item.kwargs["dbx_client"] is mock_dbx_client
            assert call_item.kwargs["s3_client"] is None


def test_date_range_databricks_mode_initializes_dbx_client_only(monkeypatch):
    mock_dbx_client = MagicMock()
    monkeypatch.setattr("spark_jobs.gen_and_ingestion_runner.ENV_MODE", "databricks")

    with (
        patch(
            "spark_jobs.gen_and_ingestion_runner.get_databricks_client",
            return_value=mock_dbx_client,
        ) as mock_get_dbx,
        patch("spark_jobs.gen_and_ingestion_runner.get_s3_client") as mock_get_s3,
        patch("spark_jobs.gen_and_ingestion_runner.run_daily_ingestion") as mock_daily,
    ):
        mock_daily.return_value = {"pending_buffer": []}

        run_date_range_ingestion(start_date=date(2026, 11, 1))

        mock_get_dbx.assert_called_once()
        mock_get_s3.assert_not_called()
        assert mock_daily.call_args.kwargs["dbx_client"] is mock_dbx_client


def test_pending_buffer_propagates_to_next_day():
    mock_s3_client = MagicMock(spec=S3Client)
    dummy_delayed_tx = {"transaction": {"transaction_id": "trx_delayed_001"}}

    with patch("spark_jobs.gen_and_ingestion_runner.run_daily_ingestion") as mock_daily:
        mock_daily.side_effect = [
            {"target_date": date(2026, 11, 1), "pending_buffer": [dummy_delayed_tx]},
            {"target_date": date(2026, 11, 2), "pending_buffer": []},
        ]

        results = run_date_range_ingestion(
            start_date=date(2026, 11, 1),
            end_date=date(2026, 11, 2),
            s3_client=mock_s3_client,
        )

        assert len(results) == 2

        first_call_buffer = mock_daily.call_args_list[0].kwargs["pending_buffer"]
        assert first_call_buffer == []

        second_call_buffer = mock_daily.call_args_list[1].kwargs["pending_buffer"]
        assert second_call_buffer == [dummy_delayed_tx]


def test_run_daily_ingestion_custom_bucket_name(tmp_path: Path, monkeypatch):
    mock_s3_client = MagicMock(spec=S3Client)
    test_date = date(2026, 11, 1)
    monkeypatch.setattr("spark_jobs.gen_and_ingestion_runner.ENV_MODE", "local")

    with patch(
        "spark_jobs.gen_and_ingestion_runner.upload_file_to_bucket"
    ) as mock_upload:
        mock_upload.side_effect = [
            "s3a://custom-bucket/bronze/webhooks/year=2026/month=11/day=01/wh.json",
            "s3a://custom-bucket/bronze/settlements/year=2026/month=11/day=01/st.csv",
        ]

        result = run_daily_ingestion(
            target_date=test_date,
            num_records=5,
            output_dir=tmp_path,
            bucket_name="custom-bucket",
            s3_client=mock_s3_client,
        )

        assert result["webhook_s3_uri"].startswith("s3a://custom-bucket/")
        webhook_key = mock_upload.call_args_list[0].args[2]
        assert webhook_key.startswith("bronze/webhooks/year=2026/month=11/day=01/")
        assert not webhook_key.startswith("s3a://")


def test_run_daily_ingestion_unsupported_mode_raises_error(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "spark_jobs.gen_and_ingestion_runner.ENV_MODE", "unsupported_cloud"
    )
    with pytest.raises(ValueError, match="Unsupported ENV_MODE"):
        run_daily_ingestion(target_date=date(2026, 11, 1), output_dir=tmp_path)
