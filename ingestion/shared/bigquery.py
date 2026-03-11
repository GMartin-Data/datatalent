"""Load file from GCS into BigQuery raw tables.

Handles format detection (JSON/Parquet), schema autodetection,
idempotent writes (WRITE_TRUNCATE), and automatic _ingestion_date stamping.
"""

from google.cloud import bigquery
from shared.logging import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger(__name__)

# Mapping extension → BigQuery source format.
# BigQuery expects NEWLINE_DELIMITED_JSON (one JSON object per line), not a JSON array.
_FORMAT_MAP: dict[str, bigquery.SourceFormat] = {
    ".json": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    ".jsonl": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    ".parquet": bigquery.SourceFormat.PARQUET,
}


def _infer_source_format(gcs_uri: str) -> bigquery.SourceFormat:
    """Infer BigQuery source format from the GCS URI file extension.

    Args:
        gcs_uri: full GCS URI (e.g. gs://datatalent-raw/sirene/2026-03-11/stock.parquet).

    Returns:
        Corresponding BigQuery SourceFormat.

    Raises:
        ValueError: if the extension is not supported.
    """
    for ext, fmt in _FORMAT_MAP.items():
        if gcs_uri.lower().endswith(ext):
            return fmt
    raise ValueError(f"Unsupported file format in URI: {gcs_uri}")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, max=10),
    reraise=True,
)
def load_gcs_to_bq(gcs_uri: str, dataset: str, table: str) -> None:
    """Load a GCS file into a BigQuery table.

    Args:
        gcs_uri: URI returned by upload_to_gcs (e.g. gs://datatalent-raw/geo/2026-03-11/regions.json).
        dataset: BigQuery dataset name (e.g. "raw").
        table: BigQuery table name (e.g. "france_travail").

    The function:
    1. Loads the file with WRITE_TRUNCATE (idempotent - full replace on each run).
    2. Adds an _ingestion_date column set to CURRENT_DATE().
       This is done via ALTER TABLE + UPDATE after the load, so source scripts
       don't need to include the field themselves.

    Raises:
        ValueError: if the file extension is not .json, .jsonl, or .parquet.
        google.api_core.exceptions.GoogleAPIError: after 3 failed attempts.
    """
    source_format = _infer_source_format(gcs_uri)

    job_config = bigquery.LoadJobConfig(
        source_format=source_format,
        autodetect=True,  # Let BigQuery infer the schema
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Idempotent
    )

    client = bigquery.Client()
    table_ref = f"{dataset}.{table}"

    # Step 1: Load file from GCS into BigQuery (replaces table contents)
    job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result()  # Block until the load job completes

    # Step 2: Stamp all rows with today's date
    # WRITE_TRUNCATE guarantees all rows come from this load,
    # so an unqualified UPDATE is safe and idempotent.
    client.query(
        f"ALTER TABLE `{table_ref}` ADD COLUMN IF NOT EXISTS _ingestion_date DATE"
    ).result()
    client.query(
        f"UPDATE `{table_ref}` SET _ingestion_date = CURRENT_DATE() WHERE TRUE"
    ).result()

    logger.info("gcs_loaded_to_bq", gcs_uri=gcs_uri, table=table_ref)
