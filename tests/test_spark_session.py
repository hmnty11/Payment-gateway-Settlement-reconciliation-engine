import pytest
from pyspark.sql import SparkSession

from payment_recon.config import MINIO_ENDPOINT
from payment_recon.spark_session import get_spark_session


def test_get_spark_session_local():
    """Verify local mode returns a configured SparkSession instance."""
    spark = get_spark_session(env_mode="local")
    try:
        assert isinstance(spark, SparkSession)
    finally:
        spark.stop()


def test_get_spark_session_local_minio_config():
    """Verify local mode configures Hadoop S3A endpoint pointing to MinIO."""
    spark = get_spark_session(env_mode="local")
    try:
        assert spark.conf.get("spark.hadoop.fs.s3a.endpoint") == MINIO_ENDPOINT
    finally:
        spark.stop()


def test_get_spark_session_mismatch_mode():
    """Verify ValueError is raised when an unsupported environment mode is provided."""
    with pytest.raises(ValueError):
        get_spark_session(env_mode="aws")
