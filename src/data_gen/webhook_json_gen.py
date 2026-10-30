import random
from datetime import UTC

from faker import Faker

fake = Faker()


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
            "currency": random.choice(["IDR", "USD", "SGD", "EUR"]),
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

    return webhook_json


def generate_batch_webhook(count: int = 10, anomaly_rate: float = 0.05) -> list[dict]:

    batch_webhook = [generate_single_webhook() for _ in range(count)]

    return batch_webhook
