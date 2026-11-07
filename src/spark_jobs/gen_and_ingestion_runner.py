from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from data_gen.settlement_bank_csv_gen import (
    generate_batch_settlement,
    save_batch_csv,
)
from data_gen.webhook_json_gen import (
    generate_batch_webhook,
    save_batch_to_json,
)
from ingestion.storage_uploader import get_s3_client, upload_file_to_bucket


def split_webhooks_for_settlement(
    webhooks: list[dict],
    late_rate: float = 0.05,
) -> tuple[list[dict], list[dict]]:
    """Split valid successful webhooks into same-day settlements and next-day pending buffer."""
    settle_today = []
    pending = []
    for wh in webhooks:
        if (
            wh.get("event", {}).get("status") == "success"
            and wh.get("transaction", {}).get("transaction_id")
            and (wh.get("transaction", {}).get("amount") or 0) > 0
        ):
            # Hold back a fraction of valid webhooks to simulate T+1 settlement delay.
            (pending if random.random() < late_rate else settle_today).append(wh)
    return settle_today, pending


def run_daily_ingestion(
    target_date: date,
    num_records: int = 100,
    output_dir: Path | str = "./data_sample",
    bucket_name: str = "payment-recon",
    pending_buffer: list[dict] | None = None,
    s3_client: S3Client | None = None,
) -> dict:
    """Generate daily webhooks and settlements, save locally, and upload to S3/MinIO."""
    s3 = s3_client or get_s3_client()
    out = Path(output_dir)
    partition = f"year={target_date:%Y}/month={target_date:%m}/day={target_date:%d}"

    webhooks = generate_batch_webhook(count=num_records, target_date=target_date)
    wh_file = save_batch_to_json(data=webhooks, path=out / "bronze_webhooks")
    settle_today, new_pending = split_webhooks_for_settlement(webhooks)

    settlements = generate_batch_settlement(
        webhook_data=(pending_buffer or []) + settle_today,
        settlement_date=target_date,
    )
    st_file = save_batch_csv(batch_data=settlements, path=out / "bronze_settlements")

    return {
        "target_date": target_date,
        "webhook_s3_uri": upload_file_to_bucket(
            wh_file, bucket_name, f"bronze/webhooks/{partition}/{wh_file.name}", s3
        ),
        "settlement_s3_uri": upload_file_to_bucket(
            st_file, bucket_name, f"bronze/settlements/{partition}/{st_file.name}", s3
        ),
        "pending_buffer": new_pending,
    }


def run_date_range_ingestion(
    start_date: date,
    end_date: date | None = None,
    num_records_per_day: int = 100,
    output_dir: Path | str = "./data_sample",
    bucket_name: str = "payment-recon",
    s3_client: S3Client | None = None,
) -> list[dict]:
    """Run daily ingestion across a date range, carrying pending webhooks forward each day."""
    end = end_date or start_date
    if start_date > end:
        raise ValueError("start_date cannot be greater than end_date")

    s3 = s3_client or get_s3_client()
    buffer: list[dict] = []
    results: list[dict] = []

    # sequential loop, add parallel execution only when backfilling years of data.
    for offset in range((end - start_date).days + 1):
        res = run_daily_ingestion(
            target_date=start_date + timedelta(days=offset),
            num_records=num_records_per_day,
            output_dir=output_dir,
            bucket_name=bucket_name,
            pending_buffer=buffer,
            s3_client=s3,
        )
        buffer = res["pending_buffer"]
        results.append(res)

    return results
