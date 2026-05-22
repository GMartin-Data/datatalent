"""Sequential entrypoint for the DataTalent ingestion pipeline.

Runs all source ingestions in order with best-effort error handling.
Each source runs independently - A failure in one does not block the others.
Designed to be executed as a Cloud Run job via `python main.py`.
"""

import os
import sys
from collections.abc import Callable

from adzuna.ingest import run as run_adzuna
from bmo.ingest import run as run_bmo
from france_travail.ingest import run as run_france_travail
from geo.ingest import run as run_geo
from google.cloud import run_v2
from shared.logging import get_logger
from sirene.ingest import run as run_sirene
from urssaf_effectifs.ingest import run as run_urssaf_effectifs
from urssaf_masse_salariale.ingest import run as run_urssaf_masse_salariale

logger = get_logger(__name__)

SOURCES: list[tuple[str, Callable]] = [
    ("france_travail", run_france_travail),
    ("sirene", run_sirene),
    ("adzuna", run_adzuna),
    ("urssaf_effectifs", run_urssaf_effectifs),
    ("urssaf_masse_salariale", run_urssaf_masse_salariale),
    ("bmo", run_bmo),
    ("geo", run_geo),
]

# Sources without which downstream dbt marts would be empty or stale (D108).
# A failure on any of these blocks the dbt invocation in main(); failures on
# other sources only degrade enrichment and are tolerated best-effort.
CRITICAL_SOURCES: frozenset[str] = frozenset({"france_travail", "adzuna"})

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
GCP_REGION = os.environ.get("GCP_REGION", "")
DBT_JOB_NAME = "datatalent-dbt"


def _invoke_dbt(critical_failures: set[str]) -> None:
    """Trigger the downstream dbt Cloud Run Job (fire-and-forget, D108).

    Skips invocation if any critical ingestion source failed; the dbt
    job would otherwise build marts on top of stale or empty data.
    On invocation, the Cloud Run RPC return is treated as acknowledgement
    only — failures during the actual dbt execution are surfaced by the
    Cloud Monitoring alert on `datatalent-dbt` (D105), not propagated here.
    """
    if critical_failures:
        logger.error(
            "dbt_skipped_critical_failure",
            critical_failures=sorted(critical_failures),
        )
        return

    if not GCP_PROJECT_ID or not GCP_REGION:
        logger.warning(
            "dbt_invocation_skipped_missing_env",
            gcp_project_id_set=bool(GCP_PROJECT_ID),
            gcp_region_set=bool(GCP_REGION),
        )
        return

    job_name = f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/jobs/{DBT_JOB_NAME}"
    try:
        client = run_v2.JobsClient()
        operation = client.run_job(name=job_name)
        logger.info("dbt_invoked", job=job_name, operation=operation.operation.name)
    except Exception as exc:
        logger.exception("dbt_invocation_failed", job=job_name, error=str(exc))


def main() -> None:
    """Run all ingestion sources sequentially (best-effort).

    Each source has its own try/except - a failure is logged but does
    not prevent the remaining sources from running. After the loop,
    the dbt Cloud Run Job is invoked iff no critical source failed
    (D108). Exit code 1 if at least one source failed (D19).
    """
    logger.info("ingestion_start", sources=[name for name, _ in SOURCES])

    errors: list[str] = []

    for name, run_fn in SOURCES:
        try:
            logger.info("source_start", source=name)
            run_fn()
            logger.info("source_end", source=name)
        except Exception as exc:
            logger.exception("source_failed", source=name, error=str(exc))
            errors.append(name)

    _invoke_dbt(set(errors) & CRITICAL_SOURCES)

    if errors:
        logger.error("ingestion_partial_failure", failed=errors)
        sys.exit(1)

    logger.info("ingestion_end")


if __name__ == "__main__":
    main()
