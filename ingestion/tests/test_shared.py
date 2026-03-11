"""Tests for ingestion.shared.logging."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import structlog.testing
from shared.gcs import upload_to_gcs
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


class TestUploadToGcs:
    """Tests for upload_to_gcs()."""

    @patch("shared.gcs.storage.Client")
    def test_returns_gcs_uri(self, mock_client_cls: MagicMock, tmp_path: Path) -> None:
        """upload_to_gcs returns a well-formed gs:// URI."""
        test_file = tmp_path / "offres.json"
        test_file.write_text('{"data": true}')

        uri = upload_to_gcs(str(test_file), "france_travail")

        assert uri.startswith("gs://datatalent-raw/france_travail/")
        assert uri.endswith("/offres.json")

    @patch("shared.gcs.storage.Client")
    def test_calls_upload_from_filename(
        self, mock_client_cls: MagicMock, tmp_path: Path
    ) -> None:
        """upload_to_gcs calls blob.upload_from_filename with the local path."""
        test_file = tmp_path / "data.json"
        test_file.write_text("{}")

        mock_blob = mock_client_cls.return_value.bucket.return_value.blob.return_value

        upload_to_gcs(str(test_file), "geo")

        mock_blob.upload_from_filename.assert_called_once_with(str(test_file))

    def test_raises_on_missing_file(self) -> None:
        """upload_to_gcs raises FileNotFoundError for nonexistent files."""
        import pytest

        with pytest.raises(FileNotFoundError):
            upload_to_gcs("/nonexistent/file.json", "sirene")
