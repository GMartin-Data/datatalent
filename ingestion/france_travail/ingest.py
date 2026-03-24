"""Orchestration de l'ingestion France Travail — extract → JSON local."""

import datetime
import json
import os
from pathlib import Path

from ingestion.shared.logging import get_logger

from .client import FranceTravailClient
from .config import CODES_ROME, DEPARTEMENTS, OUTPUT_DIR

logger = get_logger(__name__)


def run():
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    with FranceTravailClient(
        client_id=os.environ["CLIENT_ID"],
        client_secret=os.environ["CLIENT_SECRET"],
    ) as client:
        all_offres = {}

        for code in CODES_ROME:
            for dept in DEPARTEMENTS:
                logger.info("ingestion_start", code_rome=code, departement=dept)
                offres = client.fetch_offres(code, dept)

                for offre in offres:
                    all_offres[offre["id"]] = offre

        logger.info("dedup_complete", total=len(all_offres))

        filename = f"france_travail_{datetime.date.today().isoformat()}.jsonl"
        file_path = os.path.join(OUTPUT_DIR, filename)

        with open(file_path, "w", encoding="utf-8") as f:
            for offre in all_offres.values():
                f.write(json.dumps(offre, ensure_ascii=False) + "\n")

        logger.info("file_written", path=file_path, count=len(all_offres))


if __name__ == "__main__":
    run()
