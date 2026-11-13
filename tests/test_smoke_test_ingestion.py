"""Tests for smoke_test_ingestion script logic and assertions."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from scripts.smoke_test_ingestion import run_ingestion_smoke_test


def test_smoke_test_ingestion_local_success():
    """Verify smoke test happy path in local MinIO mode with cleanup enabled."""
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"ContentLength": 1024}

    dummy_runner_result = {
        "target_date": date(2026, 11, 13),
        "webhook_s3_uri": "s3a://payment-recon/bronze/webhooks/year=2026/wh.json",
        "settlement_s3_uri": "s3a://payment-recon/bronze/settlements/year=2026/st.csv",
        "pending_buffer": [],
    }

    with (
        patch("scripts.smoke_test_ingestion.ENV_MODE", "local"),
        patch("scripts.smoke_test_ingestion.get_s3_client", return_value=mock_s3),
        patch(
            "scripts.smoke_test_ingestion.run_daily_ingestion",
            return_value=dummy_runner_result,
        ),
    ):
        result = run_ingestion_smoke_test(
            target_date=date(2026, 11, 13), num_records=5, clean_up=True
        )

        assert result["status"] == "PASSED"
        assert result["webhook_size_bytes"] == 1024
        assert result["settlement_size_bytes"] == 1024
        assert mock_s3.delete_object.call_count == 2


def test_smoke_test_ingestion_local_without_cleanup():
    """Verify smoke test preserves objects when clean_up=False."""
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"ContentLength": 512}

    dummy_runner_result = {
        "target_date": date(2026, 11, 13),
        "webhook_s3_uri": "s3a://payment-recon/bronze/webhooks/year=2026/wh.json",
        "settlement_s3_uri": "s3a://payment-recon/bronze/settlements/year=2026/st.csv",
        "pending_buffer": [],
    }

    with (
        patch("scripts.smoke_test_ingestion.ENV_MODE", "local"),
        patch("scripts.smoke_test_ingestion.get_s3_client", return_value=mock_s3),
        patch(
            "scripts.smoke_test_ingestion.run_daily_ingestion",
            return_value=dummy_runner_result,
        ),
    ):
        result = run_ingestion_smoke_test(
            target_date=date(2026, 11, 13), num_records=5, clean_up=False
        )

        assert result["status"] == "PASSED"
        mock_s3.delete_object.assert_not_called()


def test_smoke_test_ingestion_databricks_success():
    """Verify smoke test happy path in Databricks Volumes mode."""
    mock_dbx = MagicMock()
    mock_meta = MagicMock()
    mock_meta.content_length = 2048
    mock_dbx.files.get_metadata.return_value = mock_meta

    dummy_runner_result = {
        "target_date": date(2026, 11, 13),
        "webhook_s3_uri": "/Volumes/workspace/payment_recon/bronze/webhooks/wh.json",
        "settlement_s3_uri": "/Volumes/workspace/payment_recon/bronze/settlements/st.csv",
        "pending_buffer": [],
    }

    with (
        patch("scripts.smoke_test_ingestion.ENV_MODE", "databricks"),
        patch(
            "scripts.smoke_test_ingestion.get_databricks_client", return_value=mock_dbx
        ),
        patch(
            "scripts.smoke_test_ingestion.run_daily_ingestion",
            return_value=dummy_runner_result,
        ),
    ):
        result = run_ingestion_smoke_test(
            target_date=date(2026, 11, 13), num_records=5, clean_up=True
        )

        assert result["status"] == "PASSED"
        assert result["webhook_size_bytes"] == 2048
        assert mock_dbx.files.delete.call_count == 2


def test_smoke_test_ingestion_empty_file_raises_error():
    """Verify smoke test negative case: raises AssertionError if file size is 0."""
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"ContentLength": 0}

    dummy_runner_result = {
        "target_date": date(2026, 11, 13),
        "webhook_s3_uri": "s3a://payment-recon/bronze/webhooks/year=2026/wh.json",
        "settlement_s3_uri": "s3a://payment-recon/bronze/settlements/year=2026/st.csv",
        "pending_buffer": [],
    }

    with (
        patch("scripts.smoke_test_ingestion.ENV_MODE", "local"),
        patch("scripts.smoke_test_ingestion.get_s3_client", return_value=mock_s3),
        patch(
            "scripts.smoke_test_ingestion.run_daily_ingestion",
            return_value=dummy_runner_result,
        ),
        pytest.raises(AssertionError, match="empty file detected"),
    ):
        run_ingestion_smoke_test(target_date=date(2026, 11, 13), clean_up=True)


def test_smoke_test_ingestion_unsupported_mode_raises_error():
    """Verify smoke test negative case: raises ValueError on unsupported ENV_MODE."""
    dummy_runner_result = {
        "target_date": date(2026, 11, 13),
        "webhook_s3_uri": "s3a://payment-recon/wh.json",
        "settlement_s3_uri": "s3a://payment-recon/st.csv",
        "pending_buffer": [],
    }

    with (
        patch("scripts.smoke_test_ingestion.ENV_MODE", "unknown_cloud"),
        patch(
            "scripts.smoke_test_ingestion.run_daily_ingestion",
            return_value=dummy_runner_result,
        ),
        pytest.raises(ValueError, match="Unsupported ENV_MODE"),
    ):
        run_ingestion_smoke_test(target_date=date(2026, 11, 13))
