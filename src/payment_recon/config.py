import os
from collections.abc import Mapping

from dotenv import load_dotenv

# Load variables from .env into os.environ
load_dotenv()


def detect_environment(env_source: Mapping[str, str] | None = None) -> str:
    """
    Detect whether the execution environment is 'local' or 'databricks'.

    Evaluation Hierarchy:
    1. Inspect the 'ENV_MODE' environment variable (highest priority).
    2. Fallback: Check if 'DATABRICKS_RUNTIME_VERSION' exists in environment.
    3. Default: Return 'local' when no matching indicators exist.

    Parameters
    ----------
    env_source : Mapping[str, str] | None, default=None
        Environment key-value mapping to evaluate. If None, defaults to `os.environ`.
        Allows deterministic dependency injection during unit testing.

    Returns
    -------
    str
        Normalized environment name, either 'local' or 'databricks'.
    """
    source = env_source if env_source is not None else os.environ
    raw_mode = source.get("ENV_MODE")

    if raw_mode is not None:
        raw_mode = raw_mode.strip().lower()
        return raw_mode

    elif "DATABRICKS_RUNTIME_VERSION" in source:
        return "databricks"

    else:
        return "local"


def resolve_storage_paths(
    mode: str, bucket_name: str = "payment-recon"
) -> dict[str, str]:
    """
    Resolve storage URI locations for Bronze and Silver layers based on environment.

    Storage Scheme Conventions:
    - 'local': S3A URI format (`s3a://<bucket_name>/...`) targeting MinIO.
    - 'databricks': Databricks Unity Catalog Volume path (`/Volumes/main/payment_recon/...`).

    Parameters
    ----------
    mode : str
        Execution mode ('local' or 'databricks'). Case-insensitive.
    bucket_name : str, default="payment-recon"
        Target MinIO bucket name for local object storage.

    Returns
    -------
    dict[str, str]
        Mapping containing resolved absolute paths for 'webhooks', 'settlements',
        'fx_rates', and 'silver'.

    Raises
    ------
    ValueError
        If `mode` is neither 'local' nor 'databricks'.
    """
    normalized_mode = mode.strip().lower()

    if normalized_mode not in ("local", "databricks"):
        raise ValueError("mode inputted not supported, use 'local' or 'databricks'")

    # base path
    if normalized_mode == "local":
        base_path = f"s3a://{bucket_name}"

    elif normalized_mode == "databricks":
        base_path = "/Volumes/main/payment_recon"

    storage_paths = {
        "webhooks": f"{base_path}/raw/webhooks",
        "settlements": f"{base_path}/raw/settlements",
        "fx_rates": f"{base_path}/raw/fx_rates",
        "silver": f"{base_path}/silver",
    }

    return storage_paths


# ===============================================================
#   Module-level constants for global import across packages
# ===============================================================

# Active environment mode detected at import time
ENV_MODE: str = detect_environment()

# MinIO credentials and endpoint configuration (local development)
MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ROOT_USER", "")
MINIO_SECRET_KEY: str = os.getenv("MINIO_ROOT_PASSWORD", "")
STORAGE_BUCKET_NAME: str = os.getenv("STORAGE_BUCKET_NAME", "payment-recon")

# Resolved storage path registry
STORAGE_PATHS: dict[str, str] = resolve_storage_paths(ENV_MODE, STORAGE_BUCKET_NAME)

RAW_WEBHOOKS_PATH: str = STORAGE_PATHS.get("webhooks", "")
RAW_SETTLEMENTS_PATH: str = STORAGE_PATHS.get("settlements", "")
RAW_FX_RATES_PATH: str = STORAGE_PATHS.get("fx_rates", "")
SILVER_PATH: str = STORAGE_PATHS.get("silver", "")
