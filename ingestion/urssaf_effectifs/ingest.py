import json
from datetime import date

from google.cloud import bigquery
from shared.bigquery import load_gcs_to_bq
from shared.gcs import upload_to_gcs
from shared.logging import get_logger
from urssaf_effectifs.client import fetch_records
from urssaf_effectifs.config import (
    BQ_DATASET,
    BQ_TABLE,
    DIMENSION_FIELDS,
    GCS_PREFIX,
    LOCAL_PATH,
    PATTERN_EFFECTIFS,
)

logger = get_logger(__name__)

SCHEMA = [
    bigquery.SchemaField("code_commune", "STRING"),
    bigquery.SchemaField("intitule_commune", "STRING"),
    bigquery.SchemaField("code_departement", "STRING"),
    bigquery.SchemaField("code_ape", "STRING"),
    bigquery.SchemaField("annee", "INTEGER"),
    bigquery.SchemaField("nb_etablissements", "INTEGER"),
    bigquery.SchemaField("effectifs_salaries", "INTEGER"),
    bigquery.SchemaField("_ingestion_date", "DATE"),
]


def _unpivot(records: list[dict]) -> list[dict]:
    """Convertit les records de wide (commune x APE) en long (commune x APE x année).

    Ignore les années où effectifs ET établissements sont tous les deux null.
    """
    long_records = []

    for record in records:
        dimensions = {field: record[field] for field in DIMENSION_FIELDS}

        # Détection des années présentes dans les champs effectifs
        years = []
        for col in record:
            match = PATTERN_EFFECTIFS.match(col)
            if match:
                years.append(int(match.group(1)))

        for year in years:
            effectifs = record.get(f"effectifs_salaries_{year}")
            nb_etab = record.get(f"nombre_d_etablissements_{year}")

            # Skip les années où il n'y a ni effectifs ni établissements
            if effectifs is None and nb_etab is None:
                continue

            long_records.append(
                {
                    **dimensions,
                    "annee": year,
                    "effectifs_salaries": effectifs,
                    "nb_etablissements": nb_etab,
                }
            )

    return long_records


def _write_jsonl(records: list[dict], path: str) -> None:
    """Écrit une liste de dicts en JSONL (une ligne par record)."""
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def run() -> None:
    """Point d'entrée principal - appelé par main.py."""
    logger.info("ingestion_start")

    raw_records = fetch_records()
    logger.info("fetched", count=len(raw_records))

    long_records = _unpivot(raw_records)
    logger.info("unpivoted", count=len(long_records))

    today = str(date.today())
    for record in long_records:
        record["_ingestion_date"] = today

    _write_jsonl(long_records, LOCAL_PATH)
    logger.info("jsonl_written", path=LOCAL_PATH)

    gcs_uri = upload_to_gcs(LOCAL_PATH, GCS_PREFIX)
    logger.info("gcs_uploaded", uri=gcs_uri)

    load_gcs_to_bq(gcs_uri, BQ_DATASET, BQ_TABLE, schema=SCHEMA)
    logger.info("bq_loaded", table=f"{BQ_DATASET}.{BQ_TABLE}")

    logger.info("ingestion_end")


if __name__ == "__main__":
    import sys

    try:
        run()
    except Exception as exc:
        logger.exception("ingestion_failed", error=str(exc))
        sys.exit(1)
