import csv
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()

fx_rates = {
    "IDR": 1.0,
    "USD": 18147.0,
    "SGD": 14174.0,
    "EUR": 20293.0,
}

bank_names = ["BCA", "MANDIRI", "AMEX", "BRI", "JAGO", "OVO"]


def single_settlement(
    webhook_data: dict | None = None, is_anomaly: bool = False
) -> dict:
    """
    Generate a single synthetic settlement record matching bank statement schema.

    Derives attributes from an existing webhook event dictionary or synthesizes
    standalone randomized transaction data using Faker. Optionally injects
    synthetic financial anomalies (amount mismatch, ghost payment, or late settlement).

    Parameters
    ----------
    webhook_data : dict | None, default=None
        Pre-existing webhook payload from which to derive transaction ID, amount,
        currency, and timestamp. If None, random values are generated via Faker.
    is_anomaly : bool, default=False
        Whether to inject an intentional anomaly into the generated record.

    Returns
    -------
    dict
        Settlement record conforming to bank mutation contract (keys: 'settlement_id',
        'transaction_id', 'transaction_date', 'settlement_date', 'gross_amount',
        'fee_amount', 'net_settled_amount', 'settlement_currency', 'applied_fx_rate',
        'bank_name').
    """

    # Branch input values based on presence of webhook data
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

    # financial calculations and date
    fx_rate = fx_rates.get(currency, 1.0)
    gross_amount = round(raw_amount * fx_rate, 2)
    fee_amount = round(gross_amount * random.uniform(0.01, 0.015), 2)
    net_settled_amount = round(gross_amount - fee_amount, 2)
    settlement_date = tx_datetime + timedelta(days=random.randint(1, 2))

    # generates anomalies
    if is_anomaly:
        anomaly = random.choice(["amount_mismatch", "ghost_payment", "late_settlement"])

        if anomaly == "amount_mismatch":
            gross_amount = round(
                gross_amount + gross_amount * random.uniform(0.05, 0.15), 2
            )

        elif anomaly == "ghost_payment":
            transaction_id = "trx_" + fake.hexify(text="^^^^^^^^^^^^")

        elif anomaly == "late_settlement":
            settlement_date = tx_datetime + timedelta(days=random.randint(4, 14))

    # dictionary
    settlement_data = {
        "settlement_id": "stl_" + fake.hexify(text="^^^^^^^^^^^^"),
        "transaction_id": transaction_id,
        "transaction_date": tx_datetime.strftime("%Y-%m-%d"),
        "settlement_date": settlement_date.strftime("%Y-%m-%d"),
        "gross_amount": gross_amount,
        "fee_amount": fee_amount,
        "net_settled_amount": net_settled_amount,
        "settlement_currency": "IDR",
        "applied_fx_rate": fx_rate,
        "bank_name": random.choice(bank_names),
    }

    return settlement_data


def generate_batch_settlement(
    webhook_data: list[dict] | None = None, count: int = 10, anomaly_rate: float = 0.05
) -> list[dict]:
    """
    Generate a collection of synthetic bank settlement records.

    Iterates over supplied webhook records or synthesizes a specified count
    of randomized settlement records, applying the configured anomaly rate.

    Parameters
    ----------
    webhook_data : list[dict] | None, default=None
        Optional list of raw webhook events to mirror into settlements.
    count : int, default=10
        Number of settlement records to generate when `webhook_data` is None.
    anomaly_rate : float, default=0.05
        Probability threshold (0.0 to 1.0) for injecting an anomaly per record.

    Returns
    -------
    list[dict]
        List of generated settlement dictionaries.
    """
    batch_settlement = []
    if webhook_data is not None:
        for wh in webhook_data:
            result = single_settlement(
                webhook_data=wh, is_anomaly=random.random() < anomaly_rate
            )
            batch_settlement.append(result)
    else:
        for _ in range(count):
            result = single_settlement(
                webhook_data=webhook_data, is_anomaly=random.random() < anomaly_rate
            )
            batch_settlement.append(result)

    return batch_settlement


def save_batch_csv(
    batch_data: list[dict] | None = None,
    path: Path | str = "./data_sample/bronze_settlement",
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
        List of settlement dictionaries to serialize.
    path : Path | str, default="./data_sample/bronze_settlement"
        Destination directory or explicit file path.
    file_name : str | None, default=None
        Custom output filename when `path` is a directory. If None, generates
        a timestamped name (`settlement_batch_%Y%m%d_%H%M%S.csv`).

    Returns
    -------
    Path
        Resolved filesystem path of the saved CSV file.
    """
    target_path = Path(path)

    if target_path.suffix.lower() == ".csv":
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_file_path = target_path

    else:
        target_path.mkdir(parents=True, exist_ok=True)

        if file_name is None:
            file_name = datetime.now(UTC).strftime("settlement_batch_%Y%m%d_%H%M%S.csv")
        target_file_path = target_path / file_name

    if not batch_data:
        return target_file_path

    with open(target_file_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(batch_data[0].keys()))
        writer.writeheader()
        writer.writerows(batch_data)

    return target_file_path
