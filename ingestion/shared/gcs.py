"""Upload local files to Google Cloud Storage.

Handles path construction (gs://{bucket}/{prefix}/{date}/{filename})
and retries on transient network errors.
"""

from datetime import UTC, datetime
from pathlib import Path

from google.cloud import storage
from shared.logging import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger(__name__)

BUCKET_NAME = "datatalent-raw"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, max=10),
    reraise=True,
)
def upload_to_gcs(local_path: str, gcs_prefix: str) -> str:
    """Upload a local file to GCS.

    Args:
        local_path: Path to the local file (e.g. /tmp/offres.json).
        gcs_prefix: source prefix in the bucket (e.g. "france_travail").

    Returns:
        Full GCS URI (e.g. gs://datatalent-raw/france_travail/2026-03-11/offres.json)

    Raises:
        FileNotFoundError: if local_path does not exist.
        google.api_core.exceptions.GoogleAPIError: after 3 failed attempts.
    """
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    date_str = datetime.now(tz=UTC).strftime("%Y-%m-%d")
    blob_name = f"{gcs_prefix}/{date_str}/{path.name}"

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(path))

    gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"
    logger.info("file_uploaded", gcs_uri=gcs_uri, size_bytes=path.stat().st_size)
    return gcs_uri
