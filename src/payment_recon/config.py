import os
from collections.abc import Mapping

from dotenv import load_dotenv

load_dotenv()


def detect_environment(env_source: Mapping[str, str] | None = None) -> str:
    """
    Detect whether the runtime environment is 'local' or 'databricks'.

    Checks ENV_MODE first, falls back to DATABRICKS_RUNTIME_VERSION,
    and defaults to 'local'.

    Parameters
    ----------
    env_source : Mapping[str, str] | None, default=None
        Environment mapping to evaluate. Defaults to `os.environ`.

    Returns
    -------
    str
        Normalized environment identifier ('local' or 'databricks').
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
    Resolve storage paths for Bronze and Silver layers based on runtime mode.

    Parameters
    ----------
    mode : str
        Target runtime mode ('local' or 'databricks').
    bucket_name : str, default="payment-recon"
        Target MinIO bucket name for local S3A URIs.

    Returns
    -------
    dict[str, str]
        Mapping of layer names ('webhooks', 'settlements', 'fx_rates', 'silver')
        to resolved URIs.

    Raises
    ------
    ValueError
        If `mode` is not 'local' or 'databricks'.
    """
    normalized_mode = mode.strip().lower()

    if normalized_mode not in ("local", "databricks"):
        raise ValueError("mode inputted not supported, use 'local' or 'databricks'")

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


ENV_MODE: str = detect_environment()

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ROOT_USER", "")
MINIO_SECRET_KEY: str = os.getenv("MINIO_ROOT_PASSWORD", "")
STORAGE_BUCKET_NAME: str = os.getenv("STORAGE_BUCKET_NAME", "payment-recon")

STORAGE_PATHS: dict[str, str] = resolve_storage_paths(ENV_MODE, STORAGE_BUCKET_NAME)

RAW_WEBHOOKS_PATH: str = STORAGE_PATHS.get("webhooks", "")
RAW_SETTLEMENTS_PATH: str = STORAGE_PATHS.get("settlements", "")
RAW_FX_RATES_PATH: str = STORAGE_PATHS.get("fx_rates", "")
SILVER_PATH: str = STORAGE_PATHS.get("silver", "")
