"""Tests for Bronze to Silver Webhook PySpark transformation job."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_jobs.bronze_to_silver_webhooks import (
    flatten_and_cast_webhooks,
    quarantine_invalid_webhooks,
)


def _build_raw_df(
    spark: SparkSession,
    rows: list[dict[str, Any]],
    include_corrupt_record: bool = False,
) -> DataFrame:
    """Helper to construct a raw bronze webhook DataFrame with hierarchical JSON schema."""
    schema = [
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("environment", StringType(), True),
        StructField("log_level", StringType(), True),
        StructField(
            "actor",
            StructType(
                [
                    StructField("ip_address", StringType(), True),
                    StructField("user_id", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "event",
            StructType(
                [
                    StructField("status", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "transaction",
            StructType(
                [
                    StructField("transaction_id", StringType(), True),
                    StructField("reference_id", StringType(), True),
                    StructField("gateway_name", StringType(), True),
                    StructField("payment_method", StringType(), True),
                    StructField("amount", StringType(), True),
                    StructField("currency", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "payment_gateway_response",
            StructType(
                [
                    StructField("status_code", StringType(), True),
                    StructField("approval_code", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "trace",
            StructType(
                [
                    StructField("trace_id", StringType(), True),
                ]
            ),
            True,
        ),
    ]

    if include_corrupt_record:
        schema.append(
            StructField("_corrupt_record", StringType(), True),
        )

    return spark.createDataFrame(rows, StructType(schema))


def test_flatten_and_cast_webhooks_happy_path(spark_session: SparkSession):
    """Verify happy path: flattening nested JSON, trimming strings, casing, and type casting."""
    raw_rows = [
        {
            "event_id": "  evt_001  ",
            "timestamp": "2026-11-13T10:15:30",
            "environment": " production ",
            "log_level": " info ",
            "actor": {"ip_address": " 192.168.1.1 ", "user_id": " usr_100 "},
            "event": {"status": " SUCCESS "},
            "transaction": {
                "transaction_id": " trx_12345 ",
                "reference_id": " ref_98765 ",
                "gateway_name": " midtrans ",
                "payment_method": " bank_transfer ",
                "amount": "150000.50",
                "currency": " idr ",
            },
            "payment_gateway_response": {
                "status_code": " 200 ",
                "approval_code": " app_777 ",
            },
            "trace": {"trace_id": " trc_001 "},
        }
    ]

    df = _build_raw_df(spark_session, raw_rows)
    result_df = flatten_and_cast_webhooks(df)

    assert result_df.count() == 1
    row = result_df.collect()[0]

    # Verify trimmed strings and proper casings
    assert row["event_id"] == "evt_001"
    assert row["environment"] == "production"
    assert row["log_level"] == "info"
    assert row["ip_address"] == "192.168.1.1"
    assert row["user_id"] == "usr_100"
    assert row["event_status"] == "success"
    assert row["transaction_id"] == "trx_12345"
    assert row["reference_id"] == "ref_98765"
    assert row["gateway_name"] == "midtrans"
    assert row["payment_method"] == "bank_transfer"
    assert row["currency"] == "IDR"
    assert row["status_code"] == "200"
    assert row["approval_code"] == "app_777"
    assert row["trace_id"] == "trc_001"

    # Verify datatypes
    assert row["amount"] == Decimal("150000.50")
    assert isinstance(row["amount"], Decimal)
    assert row["timestamp"].year == 2026
    assert row["timestamp"].month == 11
    assert row["timestamp"].day == 13


def test_flatten_and_cast_webhooks_schema_datatypes(spark_session: SparkSession):
    """Verify target DataFrame strictly conforms to the expected column datatypes."""
    raw_rows = [
        {
            "event_id": "evt_002",
            "timestamp": "2026-11-13T12:00:00",
            "environment": "sandbox",
            "log_level": "warn",
            "actor": {"ip_address": "10.0.0.1", "user_id": "usr_200"},
            "event": {"status": "failed"},
            "transaction": {
                "transaction_id": "trx_67890",
                "reference_id": "ref_11223",
                "gateway_name": "xendit",
                "payment_method": "credit_card",
                "amount": "2500000.00",
                "currency": "USD",
            },
            "payment_gateway_response": {
                "status_code": "500",
                "approval_code": "app_999",
            },
            "trace": {"trace_id": "trc_002"},
        }
    ]

    df = _build_raw_df(spark_session, raw_rows)
    result_df = flatten_and_cast_webhooks(df)

    schema_dict = {field.name: field.dataType for field in result_df.schema.fields}

    assert schema_dict["event_id"] == StringType()
    assert schema_dict["timestamp"] == TimestampType()
    assert schema_dict["environment"] == StringType()
    assert schema_dict["log_level"] == StringType()
    assert schema_dict["ip_address"] == StringType()
    assert schema_dict["user_id"] == StringType()
    assert schema_dict["event_status"] == StringType()
    assert schema_dict["transaction_id"] == StringType()
    assert schema_dict["reference_id"] == StringType()
    assert schema_dict["gateway_name"] == StringType()
    assert schema_dict["payment_method"] == StringType()
    assert schema_dict["amount"] == DecimalType(18, 2)
    assert schema_dict["currency"] == StringType()
    assert schema_dict["status_code"] == StringType()
    assert schema_dict["approval_code"] == StringType()
    assert schema_dict["trace_id"] == StringType()


def test_flatten_and_cast_webhooks_null_and_boundary_handling(
    spark_session: SparkSession,
):
    """Verify boundary cases: null fields, empty strings, and empty DataFrame."""
    raw_rows = [
        {
            "event_id": "evt_boundary",
            "timestamp": None,
            "environment": "   ",
            "log_level": None,
            "actor": {"ip_address": None, "user_id": None},
            "event": {"status": None},
            "transaction": {
                "transaction_id": None,
                "reference_id": None,
                "gateway_name": None,
                "payment_method": None,
                "amount": None,
                "currency": None,
            },
            "payment_gateway_response": {
                "status_code": None,
                "approval_code": None,
            },
            "trace": {"trace_id": None},
        }
    ]

    df = _build_raw_df(spark_session, raw_rows)
    result_df = flatten_and_cast_webhooks(df)

    row = result_df.collect()[0]

    assert row["timestamp"] is None
    assert row["environment"] == ""
    assert row["log_level"] is None
    assert row["event_status"] is None
    assert row["transaction_id"] is None
    assert row["amount"] is None
    assert row["currency"] is None

    # Empty DataFrame boundary check
    empty_df = _build_raw_df(spark_session, [])
    empty_result = flatten_and_cast_webhooks(empty_df)
    assert empty_result.count() == 0


def test_flatten_and_cast_webhooks_preserves_corrupt_record_when_present(spark_session):
    """verify if _corrupt_record is exist when anomaly is inside webhooks"""

    normal_rows = [
        {
            "event_id": "evt_002",
            "timestamp": "2026-11-13T12:00:00",
            "environment": "sandbox",
            "log_level": "warn",
            "actor": {"ip_address": "10.0.0.1", "user_id": "usr_200"},
            "event": {"status": "failed"},
            "transaction": {
                "transaction_id": "trx_67890",
                "reference_id": "ref_11223",
                "gateway_name": "xendit",
                "payment_method": "credit_card",
                "amount": "2500000.00",
                "currency": "USD",
            },
            "payment_gateway_response": {
                "status_code": "500",
                "approval_code": "app_999",
            },
            "trace": {"trace_id": "trc_002"},
        }
    ]

    corrupt_rows = [
        {
            "_corrupt_record": "INVALID_ID",
            "event_id": "",
            "timestamp": "2026-11-13T12:00:00",
            "environment": "sandbox",
            "log_level": "INFO",
            "actor": {"ip_address": "10.0.0.1", "user_id": "usr_200"},
            "event": {"status": "failed"},
            "transaction": {
                "transaction_id": " ",
                "reference_id": "ref_11223",
                "gateway_name": "xendit",
                "payment_method": "credit_card",
                "amount": "2500000.00",
                "currency": "BTC",
            },
            "payment_gateway_response": {
                "status_code": "500",
                "approval_code": "app_999",
            },
            "trace": {"trace_id": "trc_002"},
        }
    ]

    corrupt_df = _build_raw_df(spark_session, corrupt_rows, True)
    flatten_df = flatten_and_cast_webhooks(corrupt_df)

    assert "_corrupt_record" in flatten_df.columns

    normal_df = _build_raw_df(spark_session, normal_rows, False)
    flatten_df = flatten_and_cast_webhooks(normal_df)

    assert "_corrupt_record" not in flatten_df.columns


def _make_valid_row(**overrides: Any) -> dict[str, Any]:
    """make 1 line for flattened webhooks"""
    rows: dict[str, Any] = {
        "event_id": "evt_001",
        "timestamp": datetime(2026, 1, 1, 1, 1, 1),
        "environment": "production",
        "log_level": "INFO",
        "ip_address": "128.123.81.1",
        "user_id": "user_123456789101",
        "event_status": "success",
        "transaction_id": "trx_12371481498",
        "reference_id": "ref_1231491481",
        "gateway_name": "midtrans",
        "payment_method": "bank_transfer",
        "amount": Decimal(255000),
        "currency": "USD",
        "status_code": "200",
        "approval_code": "APR-219931994",
        "trace_id": "12318412",
    }

    # overide row that inputted in parameter
    rows.update(overrides)

    return rows


def _build_flattened_df(
    spark: SparkSession,
    rows: list[dict[str, Any]],
    include_corrupt_record: bool = False,
) -> DataFrame:
    fields = [
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("environment", StringType(), True),
        StructField("log_level", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_status", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("reference_id", StringType(), True),
        StructField("gateway_name", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("currency", StringType(), True),
        StructField("status_code", StringType(), True),
        StructField("approval_code", StringType(), True),
        StructField("trace_id", StringType(), True),
    ]

    if include_corrupt_record:
        fields.append(
            StructField("_corrupt_record", StringType(), True),
        )

    return spark.createDataFrame(rows, StructType(fields))


def test_quarantine_splits_valid_and_corrupt_records(spark_session: SparkSession):
    rows = [_make_valid_row(_corrupt_record="MALFORMED_JSON")]
    rows.append(_make_valid_row())
    df = _build_flattened_df(spark_session, rows, True)

    normal_df, quarantined_df = quarantine_invalid_webhooks(df)
    q_row = quarantined_df.collect()[0]

    assert "_corrupt_record" not in normal_df.columns
    assert "_corrupt_record" in quarantined_df.columns
    assert "rejection_reason" in quarantined_df.columns
    assert "quarantined_at" in quarantined_df.columns

    assert len(normal_df.columns) == 16
    assert len(quarantined_df.columns) == 19

    assert normal_df.count() == 1
    assert quarantined_df.count() == 1

    assert q_row["quarantined_at"] is not None
    assert isinstance(q_row["quarantined_at"], datetime)
    assert q_row["rejection_reason"] == "MALFORMED_JSON"
    assert normal_df.collect()[0]["event_id"] == "evt_001"


@pytest.mark.parametrize("test_input", [(None), (""), (" ")])
def test_quarantine_rejects_missing_and_blank_transaction_id(
    spark_session: SparkSession, test_input
):
    row = [_make_valid_row(transaction_id=test_input)]

    df = _build_flattened_df(spark_session, row, True)

    valid_df, quarantined_df = quarantine_invalid_webhooks(df)

    q_row = quarantined_df.collect()[0]

    assert valid_df.count() == 0
    assert q_row["transaction_id"] == test_input
    assert q_row["rejection_reason"] == "MISSING_TRANSACTION_ID"


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (None, "INVALID_AMOUNT"),
        (Decimal("0.00"), "ZERO_AMOUNT"),
        (Decimal("-50.12"), "MINUS_AMOUNT"),
    ],
)
def test_quarantine_rejects_invalid_zero_and_negative_amounts(
    spark_session: SparkSession, test_input, expected
):
    row = [_make_valid_row(amount=test_input)]
    row.append(_make_valid_row(amount=Decimal("100.00")))
    df = _build_flattened_df(spark_session, row, True)

    valid_df, quarantined_df = quarantine_invalid_webhooks(df)
    q_row = quarantined_df.collect()[0]
    v_row = valid_df.collect()[0]

    assert valid_df.count() == 1
    assert v_row["amount"] == Decimal("100.00")
    assert q_row["amount"] == test_input
    assert q_row["rejection_reason"] == expected


@pytest.mark.parametrize("test_input", [(None), ("BTC"), (""), ("RUS"), (" ")])
def test_quarantine_rejects_missing_or_unsupported_currency(
    spark_session: SparkSession, test_input
):
    row = [_make_valid_row(currency=test_input)]
    df = _build_flattened_df(spark_session, row, True)
    _, quarantined_df = quarantine_invalid_webhooks(df)

    q_row = quarantined_df.collect()[0]

    assert q_row["currency"] == test_input
    assert q_row["rejection_reason"] == "INVALID_CURRENCY"
