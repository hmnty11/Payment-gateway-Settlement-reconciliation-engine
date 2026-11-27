"""Bronze to Silver PySpark processing pipeline for payment gateway webhooks."""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType, StringType, TimestampType

from payment_recon.config import BRONZE_WEBHOOKS_PATH, ENV_MODE
from payment_recon.spark_session import get_spark_session

currency = ["IDR", "USD", "SGD", "EUR"]

valid_columns = [
    "event_id",
    "timestamp",
    "environment",
    "log_level",
    "ip_address",
    "user_id",
    "event_status",
    "transaction_id",
    "reference_id",
    "gateway_name",
    "payment_method",
    "amount",
    "currency",
    "status_code",
    "approval_code",
    "trace_id",
]


def flatten_and_cast_webhooks(df: DataFrame) -> DataFrame:
    """
    Flatten nested webhook JSON structures and cast columns to target types.

    Parameters
    ----------
    df : DataFrame
        Raw Bronze DataFrame containing hierarchical webhook JSON columns.

    Returns
    -------
    DataFrame
        Flattened DataFrame conforming to the Silver Webhook Data Contract.
    """
    selected_columns = [
        F.trim(F.col("event_id")).cast(StringType()).alias("event_id"),
        F.trim(F.col("timestamp")).cast(TimestampType()).alias("timestamp"),
        F.trim(F.col("environment")).cast(StringType()).alias("environment"),
        F.trim(F.col("log_level")).cast(StringType()).alias("log_level"),
        F.trim(F.col("actor.ip_address")).cast(StringType()).alias("ip_address"),
        F.trim(F.col("actor.user_id")).cast(StringType()).alias("user_id"),
        F.lower(F.trim(F.col("event.status"))).cast(StringType()).alias("event_status"),
        F.trim(F.col("transaction.transaction_id"))
        .cast(StringType())
        .alias("transaction_id"),
        F.trim(F.col("transaction.reference_id"))
        .cast(StringType())
        .alias("reference_id"),
        F.trim(F.col("transaction.gateway_name"))
        .cast(StringType())
        .alias("gateway_name"),
        F.trim(F.col("transaction.payment_method"))
        .cast(StringType())
        .alias("payment_method"),
        F.col("transaction.amount").cast(DecimalType(18, 2)).alias("amount"),
        F.upper(F.trim(F.col("transaction.currency")))
        .cast(StringType())
        .alias("currency"),
        F.trim(F.col("payment_gateway_response.status_code"))
        .cast(StringType())
        .alias("status_code"),
        F.trim(F.col("payment_gateway_response.approval_code"))
        .cast(StringType())
        .alias("approval_code"),
        F.trim(F.col("trace.trace_id")).cast(StringType()).alias("trace_id"),
    ]

    if "_corrupt_record" in df.columns:
        selected_columns.append(
            F.trim(F.col("_corrupt_record")).cast(StringType()).alias("_corrupt_record")
        )

    return df.select(*selected_columns)


def quarantine_invalid_webhooks(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    find and split up invalid webhooks from valid webhooks data
    """

    # checker rule
    check_corrupt = F.when(
        F.col("_corrupt_record").isNotNull(), F.lit("MALFORMED_JSON")
    )
    check_transaction_id = F.when(
        (F.col("transaction_id").isNull()) | (F.trim(F.col("transaction_id")) == ""),
        F.lit("MISSING_TRANSACTION_ID"),
    )
    check_amount = (
        F.when(F.col("amount").isNull(), F.lit("INVALID_AMOUNT"))
        .when(F.col("amount") == 0, F.lit("ZERO_AMOUNT"))
        .when(F.col("amount") <= 0, F.lit("MINUS_AMOUNT"))
    )
    check_time = F.when(F.col("timestamp").isNull(), F.lit("MISSING_TIMESTAMP"))
    check_currency = F.when(
        (F.col("currency").isNull()) | (~F.col("currency").isin(currency)),
        F.lit("INVALID_CURRENCY"),
    )

    check_event_id = F.when(
        (F.col("event_id").isNull()) | (F.trim(F.col("event_id")) == ""),
        F.lit("INVALID_ID"),
    )

    rule_list = [
        check_transaction_id,
        check_amount,
        check_time,
        check_currency,
        check_event_id,
    ]

    if "_corrupt_record" in df.columns:
        rule_list.append(check_corrupt)

    # insert reason for rejection column into df
    df = df.withColumn(
        "rejection_reason",
        F.concat_ws("; ", *rule_list).cast(StringType()).alias("rejection_reason"),
    )

    # spliting up valid_df and quarantined_df
    valid_df = df.filter(df.rejection_reason == "")
    quarantined_df = df.filter(df.rejection_reason != "")

    # cleaning up and tidying

    # valid dataframe
    drop_column = ["_corrupt_record", "rejection_reason"]
    if "_corrupt_record" in df.columns:
        valid_df = valid_df.drop(*drop_column)
    else:
        valid_df = valid_df.drop("rejection_reason")

    # quarantined dataframe
    quarantined_df = quarantined_df.withColumn("quarantined_at", F.current_timestamp())
    return valid_df, quarantined_df


if __name__ == "__main__":
    spark = get_spark_session(ENV_MODE)
    raw_df = spark.read.json(
        BRONZE_WEBHOOKS_PATH, multiLine=True, recursiveFileLookup=True
    )

    print("SEBELUM (Raw Schema):")
    raw_df.printSchema()

    flattened_df = flatten_and_cast_webhooks(raw_df)

    print("SESUDAH (Flattened & Typed Schema):")
    flattened_df.printSchema()
    flattened_df.show(5, truncate=False)

    spark.stop()
