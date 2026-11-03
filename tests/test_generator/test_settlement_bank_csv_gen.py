from datetime import datetime

from data_gen.settlement_bank_csv_gen import single_settlement
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

    # checking for the required columns base on data contract
    assert set(settlement_data.keys()) == set(expected_keys)

    # checking data type every columns
    # string columns
    string_col = ["settlement_id", "transaction_id", "bank_name", "settlement_currency"]
    for col in string_col:
        assert isinstance(settlement_data[col], str)

    # float/int columns
    float_int_col = [
        "gross_amount",
        "fee_amount",
        "net_settled_amount",
        "applied_fx_rate",
    ]
    for col in float_int_col:
        assert isinstance(settlement_data[col], (float, int))

    assert settlement_data["settlement_currency"] == "IDR"  # have to be IDR

    # datetime checking (YYYY-MM-DD)
    assert datetime.strptime(settlement_data["settlement_date"], "%Y-%m-%d")
    assert datetime.strptime(settlement_data["transaction_date"], "%Y-%m-%d")


def test_settlement_matches_webhook_transaction_id():
    webhooks = generate_batch_webhook(count=5, anomaly_rate=0.0)
    target_webhook = webhooks[0]
    expected_trx_id = target_webhook["transaction"]["transaction_id"]

    settlement = single_settlement(webhook_data=target_webhook)
    assert settlement["transaction_id"] == expected_trx_id
