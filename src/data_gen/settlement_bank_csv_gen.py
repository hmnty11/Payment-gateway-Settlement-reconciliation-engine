"""
Synthetic bank settlement CSV generator for Bronze data ingestion.

Generates tabular settlement records simulating partner bank reports,
including foreign exchange conversion, bank transaction fees, settlement cycles,
and synthetic reconciliation anomalies (amount mismatch, ghost payments, late settlement).
"""

import csv
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from faker import Faker

from data_gen.webhook_json_gen import currency as SUPPORTED_CURRENCIES

# Initialize Faker instance for realistic synthetic data generation
fake = Faker()

# FX rates relative to base settlement currency (IDR)
fx_rates = dict(
    zip(SUPPORTED_CURRENCIES, [1.0, 18147.0, 14174.0, 20293.0], strict=True)
)

# Simulated partner bank and disbursement institutions
bank_names = ["BCA", "MANDIRI", "AMEX", "BRI", "JAGO", "OVO"]


def single_settlement(
    webhook_data: dict | None = None,
    is_anomaly: bool = False,
    settlement_date: date | None = None,
) -> dict:
    """
    Generate a single synthetic settlement record matching the bank statement schema.

    Can generate a settlement linked to an existing payment gateway webhook or
    produce a standalone synthetic settlement. Calculates gross amounts using
    configured FX rates, deduces partner bank transaction fees, computes net
    settled amounts, and assigns settlement dates following typical banking cycles (T+1/T+2).
    Optionally injects anomalies for reconciliation testing.

    Parameters
    ----------
    webhook_data : dict | None, default=None
        Associated payment webhook event to reconcile against. If None,
        synthetic transaction parameters are generated independently.
    is_anomaly : bool, default=False
        Whether to inject a reconciliation discrepancy (amount mismatch,
        ghost payment, or late settlement).
    settlement_date : date | None, default=None
        Explicit settlement date override. If None, derived from transaction
        timestamp plus a 1-2 day settlement window (or 4-14 days for late settlement).

    Returns
    -------
    dict
        Dictionary representing a single bank settlement row conforming to the
        Bronze Settlement Data Contract.
    """
    # Extract transaction attributes from webhook payload or generate synthetic values
    if webhook_data is not None:
        tx = webhook_data["transaction"]
        transaction_id = tx["transaction_id"]
        raw_amount = tx["amount"]
        currency = tx["currency"]
        tx_datetime = datetime.fromisoformat(webhook_data["timestamp"])
    else:
        transaction_id = "trx_" + fake.hexify(text="^^^^^^^^^^^^")
        raw_amount = round(random.uniform(10000.0, 100000.0), 2)
        currency = random.choice(list(fx_rates.keys()))
        tx_datetime = fake.date_time(UTC)

    # Apply foreign exchange conversion and calculate bank processing fees
    fx_rate = fx_rates.get(currency, 1.0)
    gross_amount = round(raw_amount * fx_rate, 2)
    fee_amount = round(gross_amount * random.uniform(0.01, 0.015), 2)
    net_settled_amount = round(gross_amount - fee_amount, 2)

    # Determine settlement date: explicit date override or banking settlement cycle (T+1 to T+2)
    if settlement_date is not None:
        settled_dt = settlement_date
    else:
        settled_dt = (tx_datetime + timedelta(days=random.randint(1, 2))).date()

    # Inject synthetic reconciliation anomalies for exception testing
    if is_anomaly:
        # Exclude late settlement anomaly when an explicit settlement date is requested
        anomaly_choices = ["amount_mismatch", "ghost_payment"]
        if settlement_date is None:
            anomaly_choices.append("late_settlement")
        anomaly = random.choice(anomaly_choices)

        if anomaly == "amount_mismatch":
            # Amount mismatch: introduce 5% to 15% discrepancy in gross amount
            gross_amount = round(
                gross_amount + gross_amount * random.uniform(0.05, 0.15), 2
            )
            net_settled_amount = round(gross_amount - fee_amount, 2)
        elif anomaly == "ghost_payment":
            # Ghost payment: assign orphan transaction ID not present in webhook events
            transaction_id = "trx_" + fake.hexify(text="^^^^^^^^^^^^")
        elif anomaly == "late_settlement":
            # Late settlement: delay settlement beyond SLA threshold (4 to 14 days)
            settled_dt = (tx_datetime + timedelta(days=random.randint(4, 14))).date()

    # Construct settlement record adhering to Bronze data contract
    settlement_data = {
        "settlement_id": "stl_" + fake.hexify(text="^^^^^^^^^^^^"),
        "transaction_id": transaction_id,
        "transaction_date": tx_datetime.strftime("%Y-%m-%d"),
        "settlement_date": settled_dt.strftime("%Y-%m-%d"),
        "gross_amount": gross_amount,
        "fee_amount": fee_amount,
        "net_settled_amount": net_settled_amount,
        "settlement_currency": "IDR",
        "applied_fx_rate": fx_rate,
        "bank_name": random.choice(bank_names),
    }

    return settlement_data


def generate_batch_settlement(
    webhook_data: list[dict] | None = None,
    count: int = 10,
    anomaly_rate: float = 0.05,
    settlement_date: date | None = None,
) -> list[dict]:
    """
    Generate a batch of synthetic bank settlement records.

    Produces settlement records corresponding to a list of payment webhooks or
    generates standalone synthetic settlements up to `count`. Configurable
    anomaly rate controls the proportion of injected reconciliation discrepancies.

    Parameters
    ----------
    webhook_data : list[dict] | None, default=None
        List of webhook event dictionaries to create matching settlements for.
        If provided, `count` is ignored in favor of the length of this list.
    count : int, default=10
        Number of synthetic settlement records to generate when `webhook_data` is None.
    anomaly_rate : float, default=0.05
        Proportion (0.0 to 1.0) of generated settlement records to contaminate
        with reconciliation anomalies.
    settlement_date : date | None, default=None
        Optional settlement date applied across all generated records in the batch.

    Returns
    -------
    list[dict]
        List of bank settlement dictionaries.
    """
    batch_settlement = []
    if webhook_data is not None:
        # Generate settlements paired with provided webhook events
        for wh in webhook_data:
            result = single_settlement(
                webhook_data=wh,
                is_anomaly=random.random() < anomaly_rate,
                settlement_date=settlement_date,
            )
            batch_settlement.append(result)
    else:
        # Generate standalone synthetic settlements when no webhook data is provided
        for _ in range(count):
            result = single_settlement(
                webhook_data=None,
                is_anomaly=random.random() < anomaly_rate,
                settlement_date=settlement_date,
            )
            batch_settlement.append(result)

    return batch_settlement


def save_batch_csv(
    batch_data: list[dict] | None = None,
    path: Path | str = "./data_sample/bronze_settlements",
    file_name: str | None = None,
) -> Path:
    """
    Persist generated bank settlement records to a local CSV file.

    Creates target directory hierarchy if not present. If `path` ends with
    '.csv', writes directly to that file path; otherwise generates a timestamped
    filename within the destination directory.

    Parameters
    ----------
    batch_data : list[dict] | None, default=None
        List of settlement dictionaries to write as CSV rows. If empty or None,
        only the destination path is resolved and returned without writing.
    path : Path | str, default="./data_sample/bronze_settlements"
        Destination directory path or direct target '.csv' file path.
    file_name : str | None, default=None
        Explicit filename to use when `path` is a directory. If None, derives a
        name from the first record's settlement date or current UTC timestamp.

    Returns
    -------
    Path
        Path object pointing to the target CSV file.
    """
    target_path = Path(path)

    # Check if target path specifies an exact file or a destination directory
    if target_path.suffix.lower() == ".csv":
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_file_path = target_path
    else:
        target_path.mkdir(parents=True, exist_ok=True)
        if file_name is None:
            if batch_data and "settlement_date" in batch_data[0]:
                dt = datetime.strptime(batch_data[0]["settlement_date"], "%Y-%m-%d")
                file_name = dt.strftime("settlement_batch_%Y%m%d_%H%M%S.csv")
            else:
                file_name = datetime.now(UTC).strftime(
                    "settlement_batch_%Y%m%d_%H%M%S.csv"
                )
        target_file_path = target_path / file_name

    # Return resolved path early if no data records to write
    if not batch_data:
        return target_file_path

    # Write CSV with header row and UTF-8 encoding
    with open(target_file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(batch_data[0].keys()))
        writer.writeheader()
        writer.writerows(batch_data)

    return target_file_path
