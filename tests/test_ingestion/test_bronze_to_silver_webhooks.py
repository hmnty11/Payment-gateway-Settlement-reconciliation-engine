from spark_jobs.bronze_to_silver_webhooks import (
    flatten_and_cast_webhooks,
    quarantine_invalid_webhooks,
    deduplicate_webhooks,
)
from data_gen.webhook_json_gen import generate_single_webhook
import pytest

from pandas import DataFrame
import pandas as pd
from pyspark.sql import Row, SparkSession

spark = SparkSession.builder.getOrCreate()


def test_flatten_and_cast_webhooks():
    json_data = generate_single_webhook()
    df = spark.createDataFrame(json_data)
    flatten_data = flatten_and_cast_webhooks(df)
