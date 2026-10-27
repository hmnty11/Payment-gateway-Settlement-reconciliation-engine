import os
from collections.abc import Mapping

from dotenv import load_dotenv

# Panggil load_dotenv() di sini agar variabel dari file .env dimuat ke os.environ
load_dotenv()


def detect_environment(env_source: Mapping[str, str] | None = None) -> str:
    """Mendeteksi apakah sistem berjalan di 'local' atau 'databricks'.

    Hierarki Evaluasi:
    1. Periksa variabel 'ENV_MODE' (prioritas tertinggi).
    2. Fallback: Periksa apakah 'DATABRICKS_RUNTIME_VERSION' ada di environment.
    3. Default: Kembalikan 'local' jika tidak ada penanda lain.

    Args:
        env_source: Sumber dictionary environment (default: None, membaca os.environ).
                    Parameter ini membuat fungsi sangat mudah diuji di Pytest.

    Returns:
        str: 'local' atau 'databricks'.
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
    """Menentukan alamat folder penyimpanan Bronze & Silver sesuai mode.

    Aturan Path:
    - Mode 'local': format s3a://<bucket_name>/...
    - Mode 'databricks': format /Volumes/main/payment_recon/...

    Args:
        mode: Nilai 'local' atau 'databricks'.
        bucket_name: Nama bucket MinIO untuk mode lokal.

    Returns:
        dict: Berisi key 'webhooks', 'settlements', 'fx_rates', dan 'silver'.
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
#   modul level constants (agar mudah di import oleh modul lain)
# ===============================================================

# Kunci mode aktif saat modul ini di-import
ENV_MODE: str = detect_environment()

# Kredensial MinIO (hanya relevan di mode lokal)
MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ROOT_USER", "")
MINIO_SECRET_KEY: str = os.getenv("MINIO_ROOT_PASSWORD", "")
STORAGE_BUCKET_NAME: str = os.getenv("STORAGE_BUCKET_NAME", "payment-recon")

# Ekspor dictionary path dan variabel path individual
STORAGE_PATHS: dict[str, str] = resolve_storage_paths(ENV_MODE, STORAGE_BUCKET_NAME)

RAW_WEBHOOKS_PATH: str = STORAGE_PATHS.get("webhooks", "")
RAW_SETTLEMENTS_PATH: str = STORAGE_PATHS.get("settlements", "")
RAW_FX_RATES_PATH: str = STORAGE_PATHS.get("fx_rates", "")
SILVER_PATH: str = STORAGE_PATHS.get("silver", "")
