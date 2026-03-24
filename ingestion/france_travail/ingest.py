"""Orchestration de l'ingestion France Travail — extract → JSON local."""

import json
import os
from pathlib import Path

from ingestion.shared import get_logger

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
        for code in CODES_ROME:
            for dept in DEPARTEMENTS:
                logger.info("ingestion_start", code_rome=code, departement=dept)
                offres = client.fetch_offres(code, dept)

                file_path = output_dir / f"offres_{code}_{dept}.json"
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(offres, f, ensure_ascii=False, indent=2)

                logger.info("file_written", path=str(file_path), count=len(offres))


if __name__ == "__main__":
    run()
