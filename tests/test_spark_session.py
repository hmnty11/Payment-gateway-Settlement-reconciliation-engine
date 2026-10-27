import pytest
from pyspark.sql import SparkSession

from payment_recon.config import MINIO_ENDPOINT
from payment_recon.spark_session import get_spark_session


def test_get_spark_session_local():
    spark = get_spark_session(env_mode="local")
    try:
        assert isinstance(spark, SparkSession)
    finally:
        spark.stop()


def test_get_spark_session_local_minio_config():
    spark = get_spark_session(env_mode="local")
    try:
        assert spark.conf.get("spark.hadoop.fs.s3a.endpoint") == MINIO_ENDPOINT
    finally:
        spark.stop()


def test_get_spark_session_mismatch_mode():
    with pytest.raises(ValueError):
        get_spark_session(env_mode="aws")
