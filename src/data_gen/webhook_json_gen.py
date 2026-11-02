import random
from datetime import UTC

from faker import Faker

fake = Faker()

currency = ["IDR", "USD", "SGD", "EUR"]


def generate_single_webhook(is_anomaly: bool = False) -> dict:

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

    batch_webhook = [
        generate_single_webhook(is_anomaly=random.random() < anomaly_rate)
        for _ in range(count)
    ]

    return batch_webhook


def save_batch_to_json(data: list[dict], path: str = "./data_sample/bronze_webhook/"):

    pass
