"""Tests for ingestion.shared.logging."""

import structlog.testing
from shared.logging import get_logger


class TestGetLogger:
    """Tests for get_logger()."""

    def test_returns_bound_logger(self) -> None:
        """get_logger returns an object with standard logging methods."""
        logger = get_logger("test_module")
        assert callable(getattr(logger, "info", None))
        assert callable(getattr(logger, "error", None))
        assert callable(getattr(logger, "warning", None))

    def test_binds_module_name(self) -> None:
        """Logger events include the bound module name."""
        with structlog.testing.capture_logs() as captured:
            logger = get_logger("my_module")
            logger.info("test_event", key="value")

        assert len(captured) == 1
        event = captured[0]
        assert event["module"] == "my_module"
        assert event["event"] == "test_event"
        assert event["key"] == "value"
        assert event["log_level"] == "info"

    def test_different_modules_are_independent(self) -> None:
        """Two loggers with different names produce separate context."""
        with structlog.testing.capture_logs() as captured:
            logger_a = get_logger("module_a")
            logger_b = get_logger("module_b")
            logger_a.info("event_a")
            logger_b.info("event_b")

        assert len(captured) == 2
        assert captured[0]["module"] == "module_a"
        assert captured[1]["module"] == "module_b"
