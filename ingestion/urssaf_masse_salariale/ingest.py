import json
from datetime import date

from shared.bigquery import load_gcs_to_bq
from shared.gcs import upload_to_gcs
from shared.logging import get_logger
from urssaf_masse_salariale.client import fetch_records
from urssaf_masse_salariale.config import (
    BQ_DATASET,
    BQ_TABLE,
    FIELD_MAP,
    GCS_PREFIX,
    LOCAL_PATH,
)

logger = get_logger(__name__)


def _transform(records: list[dict]) -> list[dict]:
    """Transforme les enregistrements bruts API en records JSONL cibles.

    - Éclate na88 en code_na88 (int) + libelle_na88 (str)
    - Renomme les colonnes via FIELD_MAP
    """
    transformed = []
    for record in records:
        na88_raw = record["secteur_na88i"]
        code_str, libelle = na88_raw.split(" ", 1)

        row = {"code_na88": int(code_str), "libelle_na88": libelle}
        for api_field, jsonl_field in FIELD_MAP.items():
            value = record[api_field]
            if jsonl_field == "annee":
                value = int(value)
            row[jsonl_field] = value

        transformed.append(row)

    return transformed


def _write_jsonl(records: list[dict], path: str) -> None:
    """Écrit une liste de dicts en JSONL (une ligne par record)."""
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def run() -> None:
    """Point d'entrée principal, appelé par main.py."""
    logger.info("ingestion_start")

    raw_records = fetch_records()
    logger.info("fetched", count=len(raw_records))

    transformed = _transform(raw_records)

    today = str(date.today())
    for record in transformed:
        record["_ingestion_date"] = today

    _write_jsonl(transformed, LOCAL_PATH)
    logger.info("jsonl_written", path=LOCAL_PATH)

    gcs_uri = upload_to_gcs(LOCAL_PATH, GCS_PREFIX)
    logger.info("gcs_uploaded", uri=gcs_uri)

    load_gcs_to_bq(gcs_uri, BQ_DATASET, BQ_TABLE)
    logger.info("bq_loaded", table=f"{BQ_DATASET}.{BQ_TABLE}")

    logger.info("ingestion_end")


if __name__ == "__main__":
    import sys

    try:
        run()
    except Exception as exc:
        logger.exception("ingestion_failed", error=str(exc))
        sys.exit(1)
