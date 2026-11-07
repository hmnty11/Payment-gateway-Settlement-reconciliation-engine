import csv
from datetime import date, datetime
from pathlib import Path

from data_gen.settlement_bank_csv_gen import (
    generate_batch_settlement,
    save_batch_csv,
    single_settlement,
)
from data_gen.webhook_json_gen import generate_batch_webhook


def test_settlement_schema_datatypes():
    settlement_data = single_settlement()
    expected_keys = [
        "settlement_id",
        "transaction_id",
        "transaction_date",
        "settlement_date",
        "gross_amount",
        "fee_amount",
        "net_settled_amount",
        "settlement_currency",
        "applied_fx_rate",
        "bank_name",
    ]

    assert set(settlement_data.keys()) == set(expected_keys)

    string_col = ["settlement_id", "transaction_id", "bank_name", "settlement_currency"]
    for col in string_col:
        assert isinstance(settlement_data[col], str)

    float_int_col = [
        "gross_amount",
        "fee_amount",
        "net_settled_amount",
        "applied_fx_rate",
    ]
    for col in float_int_col:
        assert isinstance(settlement_data[col], (float, int))

    assert settlement_data["settlement_currency"] == "IDR"
    assert datetime.strptime(settlement_data["settlement_date"], "%Y-%m-%d")
    assert datetime.strptime(settlement_data["transaction_date"], "%Y-%m-%d")


def test_settlement_matches_webhook_transaction_id():
    webhooks = generate_batch_webhook(count=5, anomaly_rate=0.0)
    target_webhook = webhooks[0]
    expected_trx_id = target_webhook["transaction"]["transaction_id"]

    settlement = single_settlement(webhook_data=target_webhook)
    assert settlement["transaction_id"] == expected_trx_id


def test_settlement_date_injection():
    test_settlement_date = date(2026, 11, 5)
    settlement = single_settlement(settlement_date=test_settlement_date)

    assert settlement["settlement_date"] == "2026-11-05"

    batch = generate_batch_settlement(count=5, settlement_date=test_settlement_date)
    for row in batch:
        assert row["settlement_date"] == "2026-11-05"


def test_settlement_anomaly():
    webhook_data = generate_batch_webhook(count=1, anomaly_rate=0.0)
    target_webhook = webhook_data[0]
    settlement = single_settlement(target_webhook, is_anomaly=True)

    is_amount_mismatch = settlement["net_settled_amount"] != round(
        settlement["gross_amount"] - settlement["fee_amount"], 2
    )
    is_ghost_payment = (
        settlement["transaction_id"] != target_webhook["transaction"]["transaction_id"]
    )

    settlement_date = datetime.strptime(settlement["settlement_date"], "%Y-%m-%d")
    transaction_date = datetime.strptime(settlement["transaction_date"], "%Y-%m-%d")
    is_late_settlement = (settlement_date - transaction_date).days > 3

    assert is_amount_mismatch or is_ghost_payment or is_late_settlement


def test_batch_settlement():
    webhook_data = generate_batch_webhook(count=5, anomaly_rate=0.0)
    settlement_data = generate_batch_settlement(
        webhook_data=webhook_data, anomaly_rate=0.0
    )

    assert len(settlement_data) == len(webhook_data)

    for row, wh in zip(settlement_data, webhook_data):
        assert isinstance(row, dict)
        assert row["transaction_id"] == wh["transaction"]["transaction_id"]

        settlement_date = datetime.strptime(row["settlement_date"], "%Y-%m-%d")
        transaction_date = datetime.strptime(row["transaction_date"], "%Y-%m-%d")

        assert (
            row["net_settled_amount"]
            == round(row["gross_amount"] - row["fee_amount"], 2)
            and (settlement_date - transaction_date).days <= 3
        )


def test_save_batch_csv(tmp_path: Path):
    batch_data = generate_batch_settlement(count=5, anomaly_rate=0.0)

    expected_keys = [
        "settlement_id",
        "transaction_id",
        "transaction_date",
        "settlement_date",
        "gross_amount",
        "fee_amount",
        "net_settled_amount",
        "settlement_currency",
        "applied_fx_rate",
        "bank_name",
    ]

    path_target = tmp_path / "test_settlement.csv"
    save_batch_csv(batch_data=batch_data, path=path_target)

    assert path_target.exists()
    with open(path_target, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        assert set(reader.fieldnames) == set(expected_keys)
        rows = list(reader)

    assert len(rows) == 5
    assert batch_data[0]["transaction_id"] == rows[0]["transaction_id"]


def test_save_batch_csv_directory_mode(tmp_path: Path):
    batch_data = generate_batch_settlement(count=3, anomaly_rate=0.0)
    target_dir = tmp_path / "bronze_settlement"

    result_path = save_batch_csv(batch_data=batch_data, path=target_dir)

    assert result_path.exists()
    assert result_path.is_file()
    assert result_path.suffix == ".csv"
