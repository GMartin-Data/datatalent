"""Structured logging configuration for the ingestion pipeline.

Configures structlog once at module level. All modules can call get_logger()
to obtain a pre-bound logger with their module name.

Environment variables:
    LOG_FORMAT: "json" (default, production) or "console" (dev, human-readable)
"""

import os
import sys

import structlog


def _configure_structlog() -> None:
    """Configure structlog processors and output format.

    Called once at module import time. The processor chain:
    1. add_log_level    → injects {"level": "info"} into the event dict
    2. TimeStamper      → injects {"timestamp": "..."} (ISO 8601, UTC)
    3. Renderer         → converts the final dict to output string
    """
    log_format = os.getenv("LOG_FORMAT", "json").lower()

    shared_processors: list[structlog.types.Processor] = [
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    if log_format == "console":
        renderer = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(0),  # DEBUG level (tunable)
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


_configure_structlog()


def get_logger(name: str) -> structlog.BoundLogger:
    """Return a structlog logger bound to the given module name.

    Args:
        name: module identifier, typically __name__.

    Returns:
        A BoundLogger with {"module": name} pre-attached to all events.
    """
    return structlog.get_logger(module=name)
