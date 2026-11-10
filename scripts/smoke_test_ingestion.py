"""Smoke test for Bronze Ingestion Tier (MinIO S3 / Databricks Volume)."""

from __future__ import annotations

import sys
import tempfile
from datetime import date
from pathlib import Path

SRC_PATH = Path(__file__).resolve().parent.parent / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from ingestion.storage_uploader import get_databricks_client, get_s3_client
from payment_recon.config import ENV_MODE, STORAGE_BUCKET_NAME
from spark_jobs.gen_and_ingestion_runner import run_daily_ingestion


def run_ingestion_smoke_test(
    target_date: date | None = None,
    num_records: int = 10,
) -> dict[str, str | int]:
    """
    Execute ingestion smoke test and verify destination storage objects.

    Parameters
    ----------
    target_date : date | None, default=None
        Target date for batch generation. Defaults to today's date.
    num_records : int, default=10
        Number of synthetic records to generate and upload.

    Returns
    -------
    dict[str, str | int]
        Summary containing test status, destination URIs, and file sizes.

    Raises
    ------
    AssertionError
        If uploaded files are empty or cannot be verified in storage.
    """
    check_date = target_date or date.today()

    with tempfile.TemporaryDirectory() as tmp_dir:
        result = run_daily_ingestion(
            target_date=check_date,
            num_records=num_records,
            output_dir=Path(tmp_dir),
        )

    wh_uri = result["webhook_s3_uri"]
    st_uri = result["settlement_s3_uri"]

    if ENV_MODE == "local":
        s3 = get_s3_client()
        wh_key = wh_uri.removeprefix(f"s3a://{STORAGE_BUCKET_NAME}/")
        st_key = st_uri.removeprefix(f"s3a://{STORAGE_BUCKET_NAME}/")

        wh_meta = s3.head_object(Bucket=STORAGE_BUCKET_NAME, Key=wh_key)
        st_meta = s3.head_object(Bucket=STORAGE_BUCKET_NAME, Key=st_key)
        wh_size = wh_meta.get("ContentLength", 0)
        st_size = st_meta.get("ContentLength", 0)
    elif ENV_MODE == "databricks":
        dbx = get_databricks_client()
        wh_meta = dbx.files.get_metadata(wh_uri)
        st_meta = dbx.files.get_metadata(st_uri)
        wh_size = wh_meta.content_length or 0
        st_size = st_meta.content_length or 0
    else:
        raise ValueError(f"Unsupported ENV_MODE: {ENV_MODE}")

    if wh_size <= 0 or st_size <= 0:
        raise AssertionError(
            f"Smoke test failed: empty file detected (wh_size={wh_size}, st_size={st_size})"
        )

    return {
        "status": "PASSED",
        "env_mode": ENV_MODE,
        "target_date": check_date.isoformat(),
        "records_generated": num_records,
        "webhook_uri": wh_uri,
        "webhook_size_bytes": wh_size,
        "settlement_uri": st_uri,
        "settlement_size_bytes": st_size,
    }


if __name__ == "__main__":
    summary = run_ingestion_smoke_test()
    print("[SMOKE TEST INGESTION] Status: PASSED")
    for key, value in summary.items():
        print(f"  {key}: {value}")
