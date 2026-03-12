import json
import os

import requests
from geo.config import BASE_URL, BUCKET_NAME, DATASET_ID, RESOURCES
from shared.bigquery import load_gcs_to_bq
from shared.gcs import upload_to_gcs
from shared.logging import get_logger

logger = get_logger(__name__)


def fetch_geo_data(resource):
    url = f"{BASE_URL}/{resource}"
    logger.info(f"Appel API : {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def run():
    for resource in RESOURCES:
        try:
            data = fetch_geo_data(resource)

            local_path = f"/tmp/geo_{resource}.json"
            jsonl_data = "\n".join(json.dumps(row) for row in data)

            with open(local_path, "w", encoding="utf-8") as f:
                f.write(jsonl_data)

            blob_name = f"geo/{resource}.json"

            upload_to_gcs(BUCKET_NAME, blob_name, jsonl_data)
            logger.info(f"Fichier GCS mis à jour : {blob_name}")

            gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"
            table_id = f"geo_{resource}"

            load_gcs_to_bq(gcs_uri, DATASET_ID, table_id)
            logger.info(f"Table BQ mise à jour : {DATASET_ID}.{table_id}")

            if os.path.exists(local_path):
                os.remove(local_path)

        except Exception as e:
            logger.error(f"Erreur ingestion {resource}: {e}", exc_info=True)


if __name__ == "__main__":
    run()
