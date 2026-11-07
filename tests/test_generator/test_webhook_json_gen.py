import json
from datetime import date, datetime
from pathlib import Path

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

    assert set(root_keys) == set(data.keys())

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

    for parent_key, expected_child_keys in nested_contracts.items():
        child_keys = data[parent_key]
        assert isinstance(child_keys, dict)
        assert set(expected_child_keys) == set(child_keys.keys())

    attempts = data["transaction"]["attempts"]
    assert isinstance(attempts, list)
    assert len(attempts) > 0

    expected_attempt_keys = {"attempt_no", "status", "attempt_time", "reason"}
    for attempt in attempts:
        assert isinstance(attempt, dict)
        assert set(expected_attempt_keys) == set(attempt.keys())


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


def test_webhook_target_date_injection():
    test_date = date(2026, 11, 1)
    single_record = generate_single_webhook(target_date=test_date)

    parsed_time = datetime.fromisoformat(single_record["timestamp"])
    assert parsed_time.date() == test_date

    attempt_time = datetime.fromisoformat(
        single_record["transaction"]["attempts"][0]["attempt_time"]
    )
    assert attempt_time.date() == test_date

    batch = generate_batch_webhook(count=5, target_date=test_date)
    for record in batch:
        record_time = datetime.fromisoformat(record["timestamp"])
        assert record_time.date() == test_date


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

    clean_data = generate_batch_webhook(count=10, anomaly_rate=0.0)
    for item in clean_data:
        assert item["event_id"].startswith("evt_")
        assert item["transaction"]["transaction_id"].startswith("trx_")
        assert item["transaction"]["reference_id"].startswith("ref_")
        assert item["transaction"]["amount"] > 0
        assert item["transaction"]["currency"] in currency

        parsed_time = datetime.fromisoformat(item["timestamp"])
        assert parsed_time is not None

    data_with_anomaly = generate_batch_webhook(count=10, anomaly_rate=1.0)
    for item in data_with_anomaly:
        tx = item["transaction"]

        assert (
            tx.get("transaction_id") is None
            or not str(tx.get("transaction_id")).startswith("trx_")
            or tx.get("amount") is None
            or not isinstance(tx.get("amount"), (int, float))
            or tx.get("amount") <= 0
            or tx.get("currency") not in currency
        ), f"failed to load anomalies: {tx}"


def test_save_batch_to_json_creates_file_and_content_matches(tmp_path: Path):
    webhook_data = generate_batch_webhook(count=3)
    test_dir = tmp_path / "bronze_webhooks"

    result_path = save_batch_to_json(data=webhook_data, path=test_dir)

    assert result_path.exists()
    assert result_path.is_file()

    with open(result_path, encoding="utf-8") as f:
        file_json = json.load(f)
    assert file_json == webhook_data


def test_save_batch_to_json_with_explicit_filepath(tmp_path: Path):
    webhook_data = generate_batch_webhook(count=2)
    explicit_file = tmp_path / "custom" / "webhook_custom.json"

    result_path = save_batch_to_json(data=webhook_data, path=explicit_file)

    assert result_path == explicit_file
    assert result_path.exists()
