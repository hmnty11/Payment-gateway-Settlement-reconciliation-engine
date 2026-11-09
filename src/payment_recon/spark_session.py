from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from payment_recon.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


def get_spark_session(env_mode: str) -> SparkSession:
    """
    Initialize or retrieve a SparkSession configured for the target environment.

    In 'local' mode, configures Delta Lake extensions, DeltaCatalog, Hadoop S3A
    connectors targeting MinIO, and AWS SDK dependencies. In 'databricks' mode,
    retrieves the active cluster session.

    Parameters
    ----------
    env_mode : str
        Target runtime mode ('local' or 'databricks').

    Returns
    -------
    SparkSession
        Configured Spark session instance.

    Raises
    ------
    ValueError
        If `env_mode` is not 'local' or 'databricks'.
    """
    builder = SparkSession.builder.appName("PaymentReconciliationEngine")

    if env_mode == "databricks":
        return builder.getOrCreate()

    elif env_mode == "local":
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            # MinIO requires path-style access.
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
        )

        builder = configure_spark_with_delta_pip(
            builder,
            extra_packages=[
                "org.apache.hadoop:hadoop-aws:3.5.0",
                "software.amazon.awssdk:bundle:2.57.4",
            ],
        )
        return builder.getOrCreate()
    else:
        raise ValueError(
            'mode not supported, mode supported is "local" and "databricks"'
        )
