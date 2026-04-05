"""Load file from GCS into BigQuery raw tables.

Handles format detection (JSON/Parquet), schema autodetection,
configurable write disposition (WRITE_TRUNCATE or WRITE_APPEND),
and automatic _ingestion_date stamping.
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

# Mapping string → BigQuery WriteDisposition enum.
_DISPOSITION_MAP: dict[str, bigquery.WriteDisposition] = {
    "WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE,
    "WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND,
}


def _infer_source_format(gcs_uri: str) -> bigquery.SourceFormat:
    """Infer BigQuery source format from the GCS URI file extension.

    Args:
        gcs_uri: full GCS URI
            (e.g. gs://datatalent-glaq-2-raw/sirene/2026-03-11/stock.parquet).

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
def load_gcs_to_bq(
    gcs_uri: str,
    dataset: str,
    table: str,
    write_disposition: str = "WRITE_TRUNCATE",
    schema: list[bigquery.SchemaField] | None = None,
    time_partitioning: bigquery.TimePartitioning | None = None,
    clustering_fields: list[str] | None = None,
) -> None:
    """Load a GCS file into a BigQuery table.

    Args:
        gcs_uri: URI returned by upload_to_gcs
            (e.g. gs://datatalent-glaq-2-raw/geo/2026-03-11/regions.json).
        dataset: BigQuery dataset name (e.g. "raw").
        table: BigQuery table name (e.g. "france_travail").
        write_disposition: "WRITE_TRUNCATE" (default, full replace) or
            "WRITE_APPEND" (accumulation for France Travail and Adzuna - D19).
        schema: optional list of BigQuery SchemaField to force column types.
            If provided, disables autodetect and enables ignore_unknown_values
            (fields present in JSON but absent from schema are silently dropped).
            Required when autodetect infers incorrect types (e.g. Corsican codes
            2A/2B inferred as INTEGER instead of STRING).
        time_partitioning: optional BigQuery TimePartitioning config.
            Example: bigquery.TimePartitioning(field="_ingestion_date").
            Applied at table creation — has no effect on existing tables.
        clustering_fields: optional list of columns to cluster by.
            Example: ["code_commune", "categorie_metier"].
            Ignored by BigQuery if time_partitioning is not set.

    The function:
    1. Loads the file with the specified write disposition.
    2. If time_partitioning is not set: adds an _ingestion_date column
       set to CURRENT_DATE(). Only stamps rows where _ingestion_date
       is NULL, so WRITE_APPEND preserves dates from previous ingestions.
       If time_partitioning is set: skipped — the source is expected
       to pre-stamp _ingestion_date in the data before GCS upload.

    Raises:
        ValueError: if the file extension or write_disposition is not supported.
        google.api_core.exceptions.GoogleAPIError: after 3 failed attempts.
    """
    source_format = _infer_source_format(gcs_uri)

    disposition = _DISPOSITION_MAP.get(write_disposition)
    if disposition is None:
        raise ValueError(
            f"Unsupported write disposition: {write_disposition}. "
            "Use WRITE_TRUNCATE or WRITE_APPEND."
        )

    job_config = bigquery.LoadJobConfig(
        source_format=source_format,
        autodetect=True,  # Let BigQuery infer the schema
        write_disposition=disposition,
    )
    if schema:
        job_config.schema = schema
        job_config.autodetect = False
        job_config.ignore_unknown_values = True  # Ignore fields in JSON not in schema
    else:
        job_config.autodetect = True
    if time_partitioning:
        job_config.time_partitioning = time_partitioning
    if clustering_fields:
        job_config.clustering_fields = clustering_fields

    client = bigquery.Client()
    table_ref = f"{dataset}.{table}"

    # Step 1: Load file from GCS into BigQuery
    job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result()  # Block until the load job completes

    # Step 2: Stamp new rows with today's date
    # Skipped when time_partitioning is set: the source pre-stamps
    # _ingestion_date in the data before upload to GCS
    if time_partitioning is None:
        client.query(
            f"ALTER TABLE `{table_ref}` ADD COLUMN IF NOT EXISTS _ingestion_date DATE"
        ).result()
        client.query(
            f"UPDATE `{table_ref}` "
            "SET _ingestion_date = CURRENT_DATE() "
            "WHERE _ingestion_date IS NULL"
        ).result()

    logger.info(
        "gcs_loaded_to_bq",
        gcs_uri=gcs_uri,
        table=table_ref,
        write_disposition=write_disposition,
        time_partitioning_field=time_partitioning.field if time_partitioning else None,
        clustering_fields=clustering_fields,
    )
