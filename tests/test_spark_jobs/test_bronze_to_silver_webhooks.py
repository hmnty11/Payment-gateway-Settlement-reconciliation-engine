"""Tests for Bronze to Silver Webhook PySpark transformation job."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_jobs.bronze_to_silver_webhooks import flatten_and_cast_webhooks


def _build_raw_df(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    """Helper to construct a raw bronze webhook DataFrame with hierarchical JSON schema."""
    schema = StructType(
        [
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
    )
    return spark.createDataFrame(rows, schema)


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
