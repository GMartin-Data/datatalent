"""Orchestration de l'ingestion France Travail — extract → JSON local → GCS → BigQuery.

La table raw.france_travail est partitionnée par _ingestion_date
(WRITE_APPEND — D19, D26).
"""

import datetime
import json
import os

import httpx
from google.cloud import bigquery
from shared.bigquery import load_gcs_to_bq
from shared.gcs import upload_to_gcs
from shared.logging import get_logger

from .client import FranceTravailClient
from .config import CODES_ROME, DEPARTEMENTS, OUTPUT_DIR

logger = get_logger(__name__)


def deduplicate_offres(offres: list[dict]) -> list[dict]:
    """Déduplique les offres par id (last-wins)."""
    seen = {}
    for offre in offres:
        seen[offre["id"]] = offre
    return list(seen.values())


def write_jsonl(offres: list[dict], file_path: str) -> None:
    """Écrit les offres au format JSONL (une ligne JSON par offre)."""
    with open(file_path, "w", encoding="utf-8") as f:
        for offre in offres:
            f.write(json.dumps(offre, ensure_ascii=False) + "\n")


def run():
    with FranceTravailClient(
        client_id=os.environ["FT_CLIENT_ID"],
        client_secret=os.environ["FT_CLIENT_SECRET"],
    ) as client:
        raw_offres = []

        for code in CODES_ROME:
            for dept in DEPARTEMENTS:
                logger.info("ingestion_start", code_rome=code, departement=dept)
                try:
                    offres = client.fetch_offres(code, dept)
                except httpx.HTTPStatusError as e:
                    # Erreur non retryable (ex: 400) — log et skip pour ne pas perdre
                    # les données déjà collectées sur les autres combinaisons
                    logger.warning(
                        "fetch_skipped",
                        code_rome=code,
                        departement=dept,
                        status_code=e.response.status_code,
                    )
                    continue
                raw_offres.extend(offres)

        unique_offres = deduplicate_offres(raw_offres)
        logger.info("dedup_complete", raw=len(raw_offres), unique=len(unique_offres))

        today = str(datetime.date.today())
        for offre in unique_offres:
            offre["_ingestion_date"] = today

        filename = f"france_travail_{datetime.date.today().isoformat()}.jsonl"
        file_path = os.path.join(OUTPUT_DIR, filename)

        write_jsonl(unique_offres, file_path)
        logger.info("file_written", path=file_path, count=len(unique_offres))

    # Upload vers GCS
    gcs_uri = upload_to_gcs(file_path, "france_travail")
    logger.info("gcs_upload_complete", gcs_uri=gcs_uri)

    # Load dans BigQuery raw (WRITE_APPEND — D19)
    load_gcs_to_bq(
        gcs_uri,
        "raw",
        "france_travail",
        write_disposition="WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(field="_ingestion_date"),
    )
    logger.info("bq_load_complete", table="raw.france_travail")


if __name__ == "__main__":
    run()
