from pathlib import Path

# Dataset officiel Base Sirene sur data.gouv
DATA_GOUV_DATASET_ID = "5b7ffc618b4c4169d30727e0"
DATA_GOUV_API_DATASET_URL = (
    f"https://www.data.gouv.fr/api/1/datasets/{DATA_GOUV_DATASET_ID}/"
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"

HTTP_TIMEOUT_SECONDS = 60
CHUNK_SIZE = 1024 * 1024  # 1 Mo
LOG_PROGRESS_INTERVAL_BYTES = 100 * 1024 * 1024  # 100 Mo

SIRENE_RESOURCES = {
    "unite_legale": {
        "resource_id": "350182c9-148a-46e0-8389-76c2ec1374a3",
        "expected_format": "parquet",
        "filename_prefix": "StockUniteLegale",
        "bq_table": "sirene_unite_legale",
    },
    "etablissement": {
        "resource_id": "a29c1297-1f92-4e2a-8f6b-8c902ce96c5f",
        "expected_format": "parquet",
        "filename_prefix": "StockEtablissement",
        "bq_table": "sirene_etablissement",
    },
}
