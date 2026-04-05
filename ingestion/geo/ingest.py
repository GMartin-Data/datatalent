"""Ingestion source Géo — snapshots des référentiels géographiques français.

Récupère régions, départements et communes depuis l'API Géo (geo.api.gouv.fr),
dépose les fichiers JSON dans GCS et charge les tables raw dans BigQuery.
"""

import json
import os
from datetime import date
from typing import Any

import httpx
from geo.config import BASE_URL, RESOURCES
from google.cloud import bigquery
from shared.bigquery import load_gcs_to_bq
from shared.gcs import upload_to_gcs
from shared.logging import get_logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = get_logger(__name__)

REGIONS_SCHEMA = [
    bigquery.SchemaField("nom", "STRING"),
    bigquery.SchemaField("code", "STRING"),
    bigquery.SchemaField("_ingestion_date", "DATE"),
]

DEPARTEMENTS_SCHEMA = [
    bigquery.SchemaField("nom", "STRING"),
    bigquery.SchemaField("code", "STRING"),
    bigquery.SchemaField("codeRegion", "STRING"),
    bigquery.SchemaField("_ingestion_date", "DATE"),
]

COMMUNES_SCHEMA = [
    bigquery.SchemaField("nom", "STRING"),
    bigquery.SchemaField("code", "STRING"),
    bigquery.SchemaField("codeDepartement", "STRING"),
    bigquery.SchemaField("codeRegion", "STRING"),
    bigquery.SchemaField("codesPostaux", "STRING", mode="REPEATED"),
    bigquery.SchemaField("population", "INTEGER"),
    bigquery.SchemaField("_ingestion_date", "DATE"),
]

SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "regions": REGIONS_SCHEMA,
    "departements": DEPARTEMENTS_SCHEMA,
    "communes": COMMUNES_SCHEMA,
}


@retry(
    retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(3),
    reraise=True,
)
def fetch_geo_data(resource: str) -> list[dict[str, Any]]:
    """Fetch geographic reference data from the French government API.

    Args:
        resource: API endpoint name (key of RESOURCES in config.py).

    Returns:
        List of geographic entities as dictionaries.

    Raises:
        httpx.HTTPStatusError: on 4xx/5xx after retries exhausted.
        httpx.TransportError: on network failure after retries exhausted.
    """
    url = f"{BASE_URL}/{resource}"
    params = {"fields": RESOURCES[resource]}

    logger.info("api_call", url=url, resource=resource)
    response = httpx.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def run() -> None:
    """Ingest all geographic resources into BigQuery raw layer.

    Fetches regions, départements, and communes from the API Géo,
    uploads JSON snapshots to GCS, and loads them into BigQuery.

    Raises:
        RuntimeError: if at least one resource failed ingestion.
    """
    logger.info("ingestion_start", resources=list(RESOURCES.keys()))

    errors: list[str] = []

    for resource in RESOURCES:
        try:
            data = fetch_geo_data(resource)

            today = str(date.today())
            for row in data:
                row["_ingestion_date"] = today

            local_path = f"/tmp/geo_{resource}.json"
            jsonl_data = "\n".join(json.dumps(row) for row in data)

            with open(local_path, "w", encoding="utf-8") as f:
                f.write(jsonl_data)

            logger.info("local_file_written", path=local_path, rows=len(data))

            gcs_uri = upload_to_gcs(local_path, "geo")

            logger.info("gcs_upload_ok", gcs_uri=gcs_uri)

            table_id = f"geo_{resource}"

            load_gcs_to_bq(gcs_uri, "raw", table_id, schema=SCHEMAS[resource])

            logger.info("bq_load_ok", table=f"raw.{table_id}")

            if os.path.exists(local_path):
                os.remove(local_path)

        except Exception as e:
            logger.error(
                "ingestion_error", resource=resource, error=str(e), exc_info=True
            )

            errors.append(resource)

    if errors:
        logger.error("ingestion_partial_failure", failed=errors)
        raise RuntimeError(f"Ingestion failed for: {', '.join(errors)}")

    logger.info("ingestion_end")


if __name__ == "__main__":
    import sys

    try:
        run()
    except Exception:
        sys.exit(1)
