import json
from datetime import datetime

from data_gen.webhook_json_gen import (
    generate_batch_webhook,
    generate_single_webhook,
    save_batch_to_json,
)


def test_generate_batch_volume():
    batch = generate_batch_webhook(20)
    count = generate_batch_webhook(0)

    assert len(batch) == 20
    assert isinstance(batch, list)

    # checking inside list is dict or not
    for i in batch:
        assert isinstance(i, dict)

    assert len(count) == 0


def test_webhook_contract_schema_keys():
    data = generate_single_webhook()

    root_keys = [
        "event_id",
        "timestamp",
        "log_level",
        "environment",
        "event",
        "transaction",
        "payment_gateway_response",
        "actor",
        "trace",
    ]

    assert set(root_keys) == set(
        data.keys()
    )  # testing every key generated is the same with keys in data schema

    # Keys Validations
    nested_contracts = {
        "event": {"category", "action", "status"},
        "transaction": {
            "transaction_id",
            "reference_id",
            "gateway_name",
            "payment_method",
            "amount",
            "currency",
            "attempts",
        },
        "payment_gateway_response": {
            "status_code",
            "gateway_status",
            "approval_code",
        },
        "actor": {"user_id", "ip_address"},
        "trace": {"trace_id", "service_name"},
    }

    # for loop to test datatype and child keys
    for parent_key, expected_child_keys in nested_contracts.items():
        child_keys = data[parent_key]

        # to check if child keys is dictionary
        assert isinstance(child_keys, dict)

        # to check child key is the same with expected
        assert set(expected_child_keys) == set(child_keys.keys())

    # Array validation, because of it is list
    attempts = data["transaction"]["attempts"]
    assert isinstance(attempts, list)
    assert len(attempts) > 0

    expected_attempt_keys = {"attempt_no", "status", "attempt_time", "reason"}
    for attempt in attempts:
        assert isinstance(attempt, dict)

        assert set(expected_attempt_keys) == set(attempt.keys())


# testing integrity for generated data so it followed data contract
def test_webhook_data_integrity():
    webhook_data = generate_single_webhook()

    assert webhook_data["event_id"].startswith("evt_")
    assert webhook_data["transaction"]["transaction_id"].startswith("trx_")
    assert webhook_data["transaction"]["reference_id"].startswith("ref_")

    assert webhook_data["transaction"]["amount"] > 0

    currency = ["IDR", "USD", "SGD", "EUR"]
    assert webhook_data["transaction"]["currency"] in currency

    parsed_time = datetime.fromisoformat(webhook_data["timestamp"])
    assert parsed_time is not None


def test_webhook_anomaly_data():
    currency = ["IDR", "USD", "SGD", "EUR"]
    for _ in range(10):
        webhook_data = generate_single_webhook(is_anomaly=True)
        tx = webhook_data["transaction"]

        assert (
            tx.get("transaction_id") is None
            or not str(tx.get("transaction_id")).startswith("trx_")
            or tx.get("amount") is None
            or not isinstance(tx.get("amount"), (int, float))
            or tx.get("amount") <= 0
            or tx.get("currency") not in currency
        ), f"failed to load anomalies: {tx}"


def test_generate_batch_anomaly_rate():
    currency = ["IDR", "USD", "SGD", "EUR"]

    data_bersih = generate_batch_webhook(anomaly_rate=0.0)
    for item in data_bersih:
        assert item["event_id"].startswith("evt_")
        assert item["transaction"]["transaction_id"].startswith("trx_")
        assert item["transaction"]["reference_id"].startswith("ref_")

        assert item["transaction"]["amount"] > 0

        assert item["transaction"]["currency"] in currency

        parsed_time = datetime.fromisoformat(item["timestamp"])
        assert parsed_time is not None

    data_rusak = generate_batch_webhook(anomaly_rate=1.0)

    for item in data_rusak:
        tx = item["transaction"]

        assert (
            tx.get("transaction_id") is None
            or not str(tx.get("transaction_id")).startswith("trx_")
            or tx.get("amount") is None
            or not isinstance(tx.get("amount"), (int, float))
            or tx.get("amount") <= 0
            or tx.get("currency") not in currency
        ), f"failed to load anomalies: {tx}"


def test_save_batch_to_json_creates_file_and_content_matches(tmp_path):
    count = 3
    data_asli = generate_batch_webhook(count)
    test_dir = tmp_path / "bronze_webhooks"

    result_path_dir = save_batch_to_json(data_asli, test_dir)

    assert result_path_dir.exists()
    assert result_path_dir.is_file()

    with open(result_path_dir, encoding="utf-8") as f:
        file_json = json.load(f)
    assert file_json == data_asli
