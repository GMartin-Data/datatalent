"""Orchestration ingestion Adzuna — client → JSONL → GCS → BigQuery."""

import json

from shared.bigquery import load_gcs_to_bq
from shared.gcs import upload_to_gcs
from shared.logging import get_logger

from .client import fetch_all_offers
from .config import (
    BQ_DATASET,
    BQ_TABLE,
    COLUMN_MAP,
    GCS_PREFIX,
    LOCAL_JSONL_PATH,
    get_credentials,
)

logger = get_logger(__name__)


def _extract_value(offer: dict, dotted_key: str):
    """Extrait une valeur depuis un dict imbriqué via notation pointée.

    Args:
        offer: dict source (offre brute Adzuna).
        dotted_key: chemin pointé, ex "company.display_name".
    """
    current = offer
    for part in dotted_key.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _map_offer(offer: dict) -> dict:
    """Applique le mapping colonnes source → JSONL sur une offre brute.

    Args:
        offer: dict source (réponse API Adzuna).

    Returns:
        Dict avec les colonnes renommées selon COLUMN_MAP.
    """
    return {
        target: _extract_value(offer, source) for source, target in COLUMN_MAP.items()
    }


def _write_jsonl(offers: list[dict], path: str) -> None:
    """Écrit une liste de dicts en JSONL."""
    with open(path, "w", encoding="utf-8") as f:
        for offer in offers:
            f.write(json.dumps(offer, ensure_ascii=False) + "\n")


def run() -> None:
    """Point d'entrée ingestion Adzuna.

    Séquence : credentials → API paginée → mapping colonnes →
    JSONL local → GCS → BigQuery (WRITE_APPEND).
    """
    logger.info("adzuna.start")

    app_id, app_key = get_credentials()

    raw_offers = fetch_all_offers(app_id, app_key)
    logger.info("adzuna.offers_fetched", count=len(raw_offers))

    mapped_offers = [_map_offer(offer) for offer in raw_offers]
    logger.info("adzuna.offers_mapped", count=len(mapped_offers))

    _write_jsonl(mapped_offers, LOCAL_JSONL_PATH)
    logger.info("adzuna.jsonl_written", path=LOCAL_JSONL_PATH)

    gcs_uri = upload_to_gcs(LOCAL_JSONL_PATH, GCS_PREFIX)
    logger.info("adzuna.gcs_uploaded", uri=gcs_uri)

    load_gcs_to_bq(gcs_uri, BQ_DATASET, BQ_TABLE)
    logger.info("adzuna.done", count=len(mapped_offers))
