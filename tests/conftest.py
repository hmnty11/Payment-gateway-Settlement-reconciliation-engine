"""Pytest configuration and session-scoped Spark fixture."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession

from payment_recon.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession]:
    """
    Provide a reusable session-scoped SparkSession for unit and transformation tests.

    Yields
    ------
    SparkSession
        Active local SparkSession configured with MinIO and Delta Lake dependencies.
    """
    spark = get_spark_session(env_mode="local")
    yield spark
    spark.stop()
