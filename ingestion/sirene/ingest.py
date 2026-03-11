"""Sirene ingestion — stub for Bloc 1 development."""

from shared.logging import get_logger

logger = get_logger(__name__)


def run() -> None:
    """Ingest Sirene stock. Not yet implemented."""
    logger.warning("sirene_skipped", reason="not yet implemented")
