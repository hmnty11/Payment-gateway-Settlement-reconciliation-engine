import random
from datetime import UTC, datetime, timedelta

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
    settlement_date = tx_datetime + timedelta(days=random.randint(1, 14))

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
