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

    # branch of based value (something that different if there is the data)
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
