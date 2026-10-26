from payment_recon.config import detect_environment, resolve_storage_paths


# ===============================================
#              detect enviroment
# ===============================================
def test_detect_environment_local(monkeypatch):
    monkeypatch.setenv("ENV_MODE", "local")

    assert detect_environment() == "local"


def test_detect_environment_databricks(monkeypatch):
    monkeypatch.setenv("ENV_MODE", "databricks")

    assert detect_environment() == "databricks"


def test_detect_environment_default_to_local(monkeypatch):
    monkeypatch.delenv("ENV_MODE", raising=False)
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

    assert detect_environment() == "local"


def test_detect_environment_fallback_databricks(monkeypatch):
    monkeypatch.delenv("ENV_MODE", raising=False)
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")

    assert detect_environment() == "databricks"


def test_detect_environment_handles_whitespace_and_casing_local(monkeypatch):
    monkeypatch.setenv("ENV_MODE", "LoCAl  ")

    assert detect_environment() == "local"


# =======================================
#           resolve storage path
# =======================================


def test_resolve_storage_paths_local():

    storage_paths = resolve_storage_paths(mode="local")

    assert storage_paths["webhooks"].startswith("s3a://payment-recon/")
    assert storage_paths["settlements"].startswith("s3a://payment-recon/")
    assert storage_paths["fx_rates"].startswith("s3a://payment-recon/")
    assert storage_paths["silver"].startswith("s3a://payment-recon/")
