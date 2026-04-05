"""Sequential entrypoint for the DataTalent ingestion pipeline.

Runs all source ingestions in order with best-effort error handling.
Each source runs independently - A failure in one does not block the others.
Designed to be executed as a Cloud Run job via `python main.py`.
"""

import sys
from collections.abc import Callable

from adzuna.ingest import run as run_adzuna
from bmo.ingest import run as run_bmo
from france_travail.ingest import run as run_france_travail
from geo.ingest import run as run_geo
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


def main() -> None:
    """Run all ingestion sources sequentially (best-effort).

    Each source has its own try/except - a failure is logged but does
    not prevent the remaining sources from running.
    Exit code 1 if at least one source failed.
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

    if errors:
        logger.error("ingestion_partial_failure", failed=errors)
        sys.exit(1)

    logger.info("ingestion_end")


if __name__ == "__main__":
    main()
