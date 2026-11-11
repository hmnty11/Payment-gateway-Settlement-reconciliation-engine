"""Bronze to Silver PySpark processing pipeline for payment gateway webhooks."""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType, StringType, TimestampType

from payment_recon.config import BRONZE_WEBHOOKS_PATH, ENV_MODE
from payment_recon.spark_session import get_spark_session


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
    return df.select(
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
    )


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
