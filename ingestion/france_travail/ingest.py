# ingestion/france_travail/ingest.py
from pathlib import Path
import json
from config import RAW_DATA_DIR
from offres import fetch_all_offres
from config import codes_rome, departements


def run():
    output_dir = Path(RAW_DATA_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    for code in codes_rome:
        for dept in departements:
            print(f"Ingestion {code} — département {dept}")
            offres = fetch_all_offres(code, dept)

            file_path = output_dir / f"offres_{code}_{dept}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(offres, f, ensure_ascii=False, indent=2)

            print(f"Stocké : {file_path} ({len(offres)} offres)")

if __name__ == "__main__":
    run()