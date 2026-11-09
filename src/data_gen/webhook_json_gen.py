"""
Synthetic payment gateway webhook generator for Bronze data ingestion.

Generates semi-structured JSON webhook payloads conforming to the Bronze Data
Contract, with support for specific event dates and DLQ anomaly injection.
"""

import json
import random
from datetime import UTC, date, datetime
from pathlib import Path

from faker import Faker

# Initialize Faker instance for realistic synthetic data generation
fake = Faker()

# Allowed currency list matching payment gateway supported currencies
currency = ["IDR", "USD", "SGD", "EUR"]


def generate_single_webhook(
    target_date: date | None = None, is_anomaly: bool = False
) -> dict:
    """
    Generate a single synthetic nested payment gateway webhook event.

    Constructs a realistic JSON payload representing an asynchronous webhook event
    containing event metadata, transaction attributes, gateway response, user actor,
    and distributed tracing identifiers. When `is_anomaly` is True, injects an
    upstream anomaly (e.g. missing transaction ID, negative/null amount, or
    unsupported currency) for Dead-Letter Queue (DLQ) validation.

    Parameters
    ----------
    target_date : date | None, default=None
        Specific calendar date for the event timestamp. If None, a random UTC
        datetime is generated.
    is_anomaly : bool, default=False
        Whether to inject an upstream schema or value anomaly into the payload.

    Returns
    -------
    dict
        Hierarchical webhook payload adhering to the Bronze Data Contract.
    """
    # Resolve timestamp: use target_date with random time or generate completely random timestamp
    if not target_date:
        timestamp = fake.date_time(UTC).isoformat()
    else:
        timestamp = datetime.combine(
            target_date, fake.time_object(), tzinfo=UTC
        ).isoformat()

    # Construct canonical Bronze webhook payload conforming to schema contract
    webhook_json = {
        "event_id": "evt_" + fake.hexify(text="^^^^^^^^^^^^"),
        "timestamp": timestamp,
        "log_level": "INFO",
        "environment": "production",
        "event": {
            "category": "payment",
            "action": "payment_status_updated",
            "status": "success",
        },
        "transaction": {
            "transaction_id": "trx_" + fake.hexify(text="^^^^^^^^^^^^"),
            "reference_id": "ref_" + fake.bothify(text="INV-########"),
            "gateway_name": random.choice(["midtrans", "stripe"]),
            "payment_method": random.choice(
                ["credit_card", "bank_transfer", "qris", "e_wallet"]
            ),
            "amount": round(random.uniform(10000.0, 100000.0), 2),
            "currency": random.choice(currency),
            "attempts": [
                {
                    "attempt_no": 1,
                    "status": "success",
                    "attempt_time": timestamp,
                    "reason": None,
                }
            ],
        },
        "payment_gateway_response": {
            "status_code": "200",
            "gateway_status": "settlement",
            "approval_code": fake.bothify(text="APR-#########"),
        },
        "actor": {
            "user_id": "user_" + fake.hexify(text="^^^^^^^^^^^^"),
            "ip_address": fake.ipv4(),
        },
        "trace": {
            "trace_id": fake.uuid4(),
            "service_name": "payment-service",
        },
    }

    # Inject upstream data anomalies for DLQ and error handling pipelines
    if is_anomaly:
        anomaly = random.choice(
            ["missing_or_invalid_id", "invalid_amount", "invalid_currency"]
        )
        if anomaly == "missing_or_invalid_id":
            webhook_json["transaction"]["transaction_id"] = None

        elif anomaly == "invalid_amount":
            webhook_json["transaction"]["amount"] = random.choice(
                [None, round(random.uniform(-1000000.0, -10000.0), 2)]
            )

        elif anomaly == "invalid_currency":
            webhook_json["transaction"]["currency"] = random.choice(
                ["BTC", "ENRE", "XXXX", "INVALID"]
            )

    return webhook_json


def generate_batch_webhook(
    count: int = 10,
    anomaly_rate: float = 0.05,
    target_date: date | None = None,
) -> list[dict]:
    """
    Generate a batch of synthetic payment gateway webhook events.

    Produces a specified count of webhook events with a configurable probability
    of synthetic upstream anomalies.

    Parameters
    ----------
    count : int, default=10
        Number of webhook event payloads to generate.
    anomaly_rate : float, default=0.05
        Proportion (0.0 to 1.0) of generated events to contaminate with anomalies.
    target_date : date | None, default=None
        Optional target date applied to every record in the batch.

    Returns
    -------
    list[dict]
        List of generated webhook event dictionaries.
    """
    # Generate batch and randomly sample anomalies based on anomaly_rate
    batch_webhook = [
        generate_single_webhook(
            target_date=target_date, is_anomaly=random.random() < anomaly_rate
        )
        for _ in range(count)
    ]

    return batch_webhook


def save_batch_to_json(
    data: list[dict],
    path: Path | str = "./data_sample/bronze_webhooks",
    file_name: str | None = None,
) -> Path:
    """
    Serialize and save a batch of webhook dictionaries to a local JSON file.

    Creates destination directories if absent, and writes formatted JSON output.
    If `path` ends with '.json', it is used as the direct destination file path;
    otherwise, a timestamped filename is generated inside the directory.

    Parameters
    ----------
    data : list[dict]
        List of webhook event dictionaries to serialize.
    path : Path | str, default="./data_sample/bronze_webhooks"
        Destination directory path or direct target '.json' file path.
    file_name : str | None, default=None
        Explicit filename to use when `path` is a directory. If None, derives a
        name from the first record's timestamp or the current UTC datetime.

    Returns
    -------
    Path
        Path object pointing to the written JSON file.
    """
    target_path = Path(path)

    # Check if target path specifies an exact file or a destination directory
    if target_path.suffix.lower() == ".json":
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_file_path = target_path
    else:
        target_path.mkdir(parents=True, exist_ok=True)
        if file_name is None:
            if data and "timestamp" in data[0]:
                dt = datetime.fromisoformat(data[0]["timestamp"])
                file_name = dt.strftime("webhook_batch_%Y%m%d_%H%M%S.json")
            else:
                file_name = datetime.now(UTC).strftime(
                    "webhook_batch_%Y%m%d_%H%M%S.json"
                )
        target_file_path = target_path / file_name

    # Write formatted JSON to file
    with open(target_file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return target_file_path


if __name__ == "__main__":
    batch = generate_batch_webhook(count=100, anomaly_rate=0.10)
    path = save_batch_to_json(data=batch)
