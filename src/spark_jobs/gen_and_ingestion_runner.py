from __future__ import annotations
from typing import TYPE_CHECKING
from datetime import date

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
from pathlib import Path
from data_gen.webhook_json_gen import generate_batch_webhook, save_batch_to_json
from data_gen.settlement_bank_csv_gen import generate_batch_settlement, save_batch_csv
from ingestion.storage_uploader import upload_file_to_bucket


def run_daily_ingestion(
    target_date: date,
    num_records: int = 100,
    output_dir: Path = "data_sample/",
    bucket_name: str = "payment-recon",
    pending_buffer: list[dict] | None = None,
    s3_client: S3Client | None = None,
) -> dict:

    pass


def run_date_range_ingestion(
    start_date: date,
    end_date: date | None = None,
    num_records_per_day: int = 100,
    output_dir: Path = "data_sample/",
    bucket_name: str = "payment_record",
    s3_client: S3Client | None = None,
) -> list:

    pass
