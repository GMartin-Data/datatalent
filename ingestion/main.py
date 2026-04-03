"""Sequential entrypoint for the DataTalent ingestion pipeline.

Runs all source ingestions in order.
Designed to be executed as a Cloud Run job via `python main.py`.
"""

import sys

from adzuna.ingest import run as run_adzuna
from bmo.ingest import run as run_bmo
from france_travail.ingest import run as run_france_travail
from geo.ingest import run as run_geo
from shared.logging import get_logger
from sirene.ingest import run as run_sirene
from urssaf_effectifs.ingest import run as run_urssaf_effectifs
from urssaf_masse_salariale.ingest import run as run_urssaf_masse_salariale

logger = get_logger(__name__)


def main() -> None:
    """Run all ingestion sources sequentially.

    If any source fails, logs the error and exits with code 1.
    Cloud Run Job interprets a non-zero exit as a failed execution.
    """
    logger.info("ingestion_start")

    try:
        run_france_travail()
        run_sirene()
        run_adzuna()
        run_urssaf_effectifs()
        run_urssaf_masse_salariale()
        run_bmo()
        run_geo()
    except Exception:
        logger.exception("ingestion_failed")
        sys.exit(1)

    logger.info("ingestion_end")


if __name__ == "__main__":
    main()
