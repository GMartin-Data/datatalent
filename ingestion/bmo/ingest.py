"""Ingestion BMO : téléchargement XLSX → parse → JSONL → GCS → BQ raw."""

import json
import tempfile
from datetime import date
from pathlib import Path

import httpx
from bmo.config import (
    BMO_XLSX_URL,
    BQ_DATASET,
    BQ_TABLE,
    GCS_PREFIX,
)
from bmo.parse_xlsx import parse_bmo_xlsx
from shared.bigquery import load_gcs_to_bq
from shared.gcs import upload_to_gcs
from shared.logging import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def _download_xlsx(url: str, dest: Path) -> None:
    """Télécharge le XLSX en streaming vers un fichier local.

    Streaming pour ne pas charger ~10Mo en RAM d'un coup.
    Retry tenacity : 3 tentatives, backoff exponentiel 2s → 4s → 8s.
    """
    with httpx.stream("GET", url, follow_redirects=True, timeout=60) as response:
        response.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in response.iter_bytes(chunk_size=8_192):
                f.write(chunk)


def _write_jsonl(records: list[dict], dest: Path) -> None:
    """Écrit les records en JSONL (un objet JSON par ligne).

    ensure_ascii=False: conserve les accents dans les libellés
    (ex: "Rhône", "fonctions d'encadrement").
    """
    with open(dest, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def run() -> None:
    """Point d'entrée du module BMO - appelé par main.py."""
    logger.info("ingestion_start")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)

        # 1. Téléchargement XLSX
        xlsx_path = tmp / "bmo.xlsx"
        logger.info("download_start", url=BMO_XLSX_URL)
        _download_xlsx(BMO_XLSX_URL, xlsx_path)
        logger.info("download_end", size_bytes=xlsx_path.stat().st_size)

        # 2. Parsing + Filtrage IT
        records = parse_bmo_xlsx(xlsx_path)
        logger.info("parse_done", records_count=len(records))

        if not records:
            logger.warning("no_records", msg="Aucune ligne IT trouvée - abandon")
            return

        # 3. Stamp _ingestion_date
        today = str(date.today())
        for record in records:
            record["_ingestion_date"] = today

        # 4. Écriture JSONL local
        jsonl_path = tmp / "bmo.jsonl"
        _write_jsonl(records, jsonl_path)

        # 5. Upload GCS
        gcs_uri = upload_to_gcs(str(jsonl_path), GCS_PREFIX)
        logger.info("gcs_uploaded", uri=gcs_uri)

        # 6. Chargement BigQuery raw
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
