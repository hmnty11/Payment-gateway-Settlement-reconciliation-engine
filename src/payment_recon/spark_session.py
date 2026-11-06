from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from payment_recon.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


def get_spark_session(env_mode: str) -> SparkSession:
    """
    Initialize or retrieve a configured SparkSession for the target environment.

    In 'databricks' mode, retrieves the pre-existing cloud SparkSession managed
    by the cluster runtime. In 'local' mode, configures Delta Lake extensions,
    DeltaCatalog, Hadoop S3A filesystem connector targeting MinIO, and AWS SDK
    bundle dependencies via Delta PIP helper.

    Parameters
    ----------
    env_mode : str
        Target runtime mode ('local' or 'databricks').

    Returns
    -------
    SparkSession
        Configured Apache Spark session instance.

    Raises
    ------
    ValueError
        If `env_mode` is neither 'local' nor 'databricks'.
    """
    builder = SparkSession.builder.appName("PaymentReconciliationEngine")

    if env_mode == "databricks":
        return builder.getOrCreate()

    elif env_mode == "local":
        builder = (
            builder
            # DELTA LAKE CONFIGURATION
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # MINIO CONFIGURATION S3A
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            # ACTIVATE 2 RULES SO IT CAN ACCESS MINIO
            .config(
                "spark.hadoop.fs.s3a.path.style.access", "true"
            )  # url configuration
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )  # spark can read s3a files (minio)
        )

        builder = configure_spark_with_delta_pip(
            builder,
            extra_packages=[
                "org.apache.hadoop:hadoop-aws:3.5.0",
                "software.amazon.awssdk:bundle:2.57.4",
            ],
        )
        # ACTIVATE SPARK SESSION
        return builder.getOrCreate()
    else:
        raise ValueError(
            'mode not supported, mode supported is "local" and "databricks"'
        )
