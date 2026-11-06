import json
import random
from datetime import UTC, datetime
from pathlib import Path

from faker import Faker

fake = Faker()

currency = ["IDR", "USD", "SGD", "EUR"]


def generate_single_webhook(is_anomaly: bool = False) -> dict:
    """
    Generate a single synthetic nested payment gateway webhook event.

    Constructs a realistic JSON payload representing an asynchronous webhook event
    containing event metadata, transaction attributes, gateway response, user actor,
    and distributed tracing identifiers. When `is_anomaly` is True, injects an
    upstream anomaly (e.g. missing transaction ID, negative/null amount, or
    unsupported currency) for Dead-Letter Queue (DLQ) validation.

    Parameters
    ----------
    is_anomaly : bool, default=False
        Whether to inject an anomaly into the webhook payload.

    Returns
    -------
    dict
        Hierarchical webhook payload adhering to the Bronze Data Contract.
    """
    webhook_json = {
        "event_id": "evt_" + fake.hexify(text="^^^^^^^^^^^^"),
        "timestamp": fake.date_time(UTC).isoformat(),
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
                    "attempt_time": "2026-10-30T01:55:58.000Z",
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


def generate_batch_webhook(count: int = 10, anomaly_rate: float = 0.05) -> list[dict]:
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

    Returns
    -------
    list[dict]
        List of generated webhook event dictionaries.
    """
    batch_webhook = [
        generate_single_webhook(is_anomaly=random.random() < anomaly_rate)
        for _ in range(count)
    ]

    return batch_webhook


def save_batch_to_json(
    data: list[dict],
    path: Path | str = "./data_sample/bronze_webhook",
    file_name: str | None = None,
) -> Path:
    """
    Serialize and save a batch of webhook dictionaries to a local JSON file.

    Creates destination directories if absent, and writes formatted JSON output.

    Parameters
    ----------
    data : list[dict]
        Collection of webhook dictionaries to serialize.
    path : Path | str, default="./data_sample/bronze_webhook"
        Target directory to store the JSON file.
    file_name : str | None, default=None
        Custom filename. If None, generates a timestamped name
        (`webhook_batch_%Y%m%d_%H%M%S.json`).

    Returns
    -------
    Path
        Path to the saved JSON file.
    """
    target_dir = Path(path)
    target_dir.mkdir(parents=True, exist_ok=True)

    if file_name is None:
        file_name = datetime.now(UTC).strftime("webhook_batch_%Y%m%d_%H%M%S.json")

    target_file_path = target_dir / file_name

    with open(target_file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return target_file_path


if __name__ == "__main__":
    batch = generate_batch_webhook(count=100, anomaly_rate=0.10)
    path = save_batch_to_json(data=batch)
