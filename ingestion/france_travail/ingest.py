"""Orchestration de l'ingestion France Travail — extract → JSON local."""

import json
import os
from pathlib import Path

from .client import FranceTravailClient
from .config import RAW_DATA_DIR, codes_rome, departements


def run():
    output_dir = Path(RAW_DATA_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    with FranceTravailClient(
        client_id=os.environ["CLIENT_ID"],
        client_secret=os.environ["CLIENT_SECRET"],
    ) as client:
        for code in codes_rome:
            for dept in departements:
                print(f"Ingestion {code} — département {dept}")
                offres = client.fetch_offres(code, dept)

                file_path = output_dir / f"offres_{code}_{dept}.json"
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(offres, f, ensure_ascii=False, indent=2)

                print(f"Stocké : {file_path} ({len(offres)} offres)")


if __name__ == "__main__":
    run()
