"""Pipeline runner to generate synthetic Bronze data and upload to object storage."""

from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from databricks.sdk import WorkspaceClient

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
from ingestion.storage_uploader import (
    get_databricks_client,
    get_s3_client,
    upload_file_to_bucket,
    upload_file_to_volume,
)
from payment_recon.config import (
    BRONZE_SETTLEMENTS_PATH,
    BRONZE_WEBHOOKS_PATH,
    DATABRICKS_VOLUME_BASE_PATH,
    ENV_MODE,
    STORAGE_BUCKET_NAME,
)


def split_webhooks_for_settlement(
    webhooks: list[dict],
    late_rate: float = 0.05,
) -> tuple[list[dict], list[dict]]:
    """
    Split valid successful webhooks into same-day settlements and next-day pending buffer.

    Parameters
    ----------
    webhooks : list[dict]
        Raw webhook event payloads to filter and partition.
    late_rate : float, default=0.05
        Fraction of successful webhooks delayed to simulate T+1 settlement lag.

    Returns
    -------
    tuple[list[dict], list[dict]]
        Pair of `(settle_today, pending)` webhook lists.
    """
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
    bucket_name: str = STORAGE_BUCKET_NAME,
    pending_buffer: list[dict] | None = None,
    s3_client: S3Client | None = None,
    dbx_client: WorkspaceClient | None = None,
) -> dict:
    """
    Generate daily webhooks and settlements, save locally, and upload to storage.

    Parameters
    ----------
    target_date : date
        Calendar date for the batch ingestion and storage partition.
    num_records : int, default=100
        Number of webhook records to generate.
    output_dir : Path | str, default="./data_sample"
        Local directory path for temporary file persistence.
    bucket_name : str, default="payment-recon"
        Destination S3 bucket name for local mode.
    pending_buffer : list[dict] | None, default=None
        Pending webhooks held over from previous dates to settle on `target_date`.
    s3_client : S3Client | None, default=None
        Boto3 S3 client. Initialized via `get_s3_client()` if None in local mode.
    dbx_client : WorkspaceClient | None, default=None
        Databricks WorkspaceClient. Initialized via `get_databricks_client()` if None in databricks mode.

    Returns
    -------
    dict
        Dictionary containing `target_date`, destination URIs for webhooks and settlements,
        and the updated `pending_buffer`.
    """
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

    if ENV_MODE == "local":
        s3 = s3_client or get_s3_client()
        data = {
            "target_date": target_date,
            "webhook_s3_uri": upload_file_to_bucket(
                wh_file,
                bucket_name,
                f"{BRONZE_WEBHOOKS_PATH.removeprefix(f's3a://{bucket_name}/')}/{partition}/{wh_file.name}",
                s3,
            ),
            "settlement_s3_uri": upload_file_to_bucket(
                st_file,
                bucket_name,
                f"{BRONZE_SETTLEMENTS_PATH.removeprefix(f's3a://{bucket_name}/')}/{partition}/{st_file.name}",
                s3,
            ),
            "pending_buffer": new_pending,
        }

    elif ENV_MODE == "databricks":
        dbx = dbx_client or get_databricks_client()
        data = {
            "target_date": target_date,
            "webhook_s3_uri": upload_file_to_volume(
                wh_file,
                f"{BRONZE_WEBHOOKS_PATH.removeprefix(f'{DATABRICKS_VOLUME_BASE_PATH}/')}/{partition}/{wh_file.name}",
                dbx_client=dbx,
            ),
            "settlement_s3_uri": upload_file_to_volume(
                st_file,
                f"{BRONZE_SETTLEMENTS_PATH.removeprefix(f'{DATABRICKS_VOLUME_BASE_PATH}/')}/{partition}/{st_file.name}",
                dbx_client=dbx,
            ),
            "pending_buffer": new_pending,
        }
    return data


def run_date_range_ingestion(
    start_date: date,
    end_date: date | None = None,
    num_records_per_day: int = 100,
    output_dir: Path | str = "./data_sample",
    bucket_name: str = STORAGE_BUCKET_NAME,
    s3_client: S3Client | None = None,
    dbx_client: WorkspaceClient | None = None,
) -> list[dict]:
    """
    Run daily ingestion across a date range, carrying pending webhooks forward each day.

    Parameters
    ----------
    start_date : date
        Starting date of the ingestion window.
    end_date : date | None, default=None
        Ending date of the ingestion window. Defaults to `start_date` if None.
    num_records_per_day : int, default=100
        Number of webhook records generated per day.
    output_dir : Path | str, default="./data_sample"
        Local directory path for temporary file persistence.
    bucket_name : str, default="payment-recon"
        Destination S3 bucket name for local mode.
    s3_client : S3Client | None, default=None
        Boto3 S3 client. Initialized via `get_s3_client()` if None in local mode.
    dbx_client : WorkspaceClient | None, default=None
        Databricks WorkspaceClient. Initialized via `get_databricks_client()` if None in databricks mode.

    Returns
    -------
    list[dict]
        List of daily ingestion outcome dictionaries in chronological order.

    Raises
    ------
    ValueError
        If `start_date` is greater than `end_date`.
    """
    end = end_date or start_date
    if start_date > end:
        raise ValueError("start_date cannot be greater than end_date")

    s3 = (s3_client or get_s3_client()) if ENV_MODE == "local" else s3_client
    dbx = (
        (dbx_client or get_databricks_client())
        if ENV_MODE == "databricks"
        else dbx_client
    )
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
            dbx_client=dbx,
        )
        buffer = res["pending_buffer"]
        results.append(res)

    return results


if __name__ == "__main__":
    run_date_range_ingestion(start_date=date(2025, 1, 1), end_date=date(2026, 1, 1))
