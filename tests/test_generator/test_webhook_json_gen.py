from data_gen.webhook_json_gen import generate_batch_webhook, generate_single_webhook


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
    for parent_key, child_keys in nested_contracts.items():
        expected_child_keys = data[parent_key]

        # to check if child keys is dictionary
        assert isinstance(expected_child_keys, dict)

        # to check child key is the same with expected
        assert set(child_keys) == set(expected_child_keys.keys())

    # Array validation, because of it is list
    attempts = data["transaction"]["attempts"]
    assert isinstance(attempts, list)
    assert len(attempts) > 0

    expected_attempt_keys = {"attempt_no", "status", "attempt_time", "reason"}
    for attempt in attempts:
        assert isinstance(attempt, dict)

        assert set(expected_attempt_keys) == set(attempt.keys())
