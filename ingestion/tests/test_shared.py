"""Tests for ingestion.shared (logging, gcs, bigquery)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import structlog.testing
from google.cloud import bigquery
from main import main
from shared.bigquery import _infer_source_format, load_gcs_to_bq
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
        with pytest.raises(FileNotFoundError):
            upload_to_gcs("/nonexistent/file.json", "sirene")


class TestInferSourceFormat:
    """Tests for _infer_source_format()."""

    def test_json_format(self) -> None:
        fmt = _infer_source_format("gs://bucket/prefix/2026-03-11/data.json")
        assert fmt == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    def test_jsonl_format(self) -> None:
        fmt = _infer_source_format("gs://bucket/prefix/2026-03-11/data.jsonl")
        assert fmt == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    def test_parquet_format(self) -> None:
        fmt = _infer_source_format("gs://bucket/prefix/2026-03-11/stock.parquet")
        assert fmt == bigquery.SourceFormat.PARQUET

    def test_unsupported_format_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported file format"):
            _infer_source_format("gs://bucket/prefix/data.csv")


class TestLoadGcsToBq:
    """Tests for load_gcs_to_bq()."""

    @patch("shared.bigquery.bigquery.Client")
    def test_calls_load_with_correct_config(self, mock_client_cls: MagicMock) -> None:
        """load_gcs_to_bq triggers a load job with WRITE_TRUNCATE and autodetect."""
        mock_client = mock_client_cls.return_value
        mock_job = mock_client.load_table_from_uri.return_value

        load_gcs_to_bq(
            "gs://datatalent-raw/geo/2026-03-11/regions.json", "raw", "geo_regions"
        )

        # Verify load_table_from_uri was called with the right URI and table.
        call_args = mock_client.load_table_from_uri.call_args
        assert call_args[0][0] == "gs://datatalent-raw/geo/2026-03-11/regions.json"
        assert call_args[0][1] == "raw.geo_regions"

        # Verify the job config.
        job_config = call_args[1]["job_config"]
        assert job_config.autodetect is True
        assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE

        # Verify job.result() was called (blocking wait).
        mock_job.result.assert_called_once()

    @patch("shared.bigquery.bigquery.Client")
    def test_stamps_ingestion_date(self, mock_client_cls: MagicMock) -> None:
        """load_gcs_to_bq runs ALTER + UPDATE to add _ingestion_date."""
        mock_client = mock_client_cls.return_value

        load_gcs_to_bq(
            "gs://datatalent-raw/sirene/2026-03-11/stock.parquet", "raw", "sirene"
        )

        # client.query() is called twice: ALTER then UPDATE.
        queries = [call[0][0] for call in mock_client.query.call_args_list]
        assert len(queries) == 2
        assert "ALTER TABLE" in queries[0]
        assert "_ingestion_date" in queries[0]
        assert "UPDATE" in queries[1]
        assert "CURRENT_DATE()" in queries[1]


class TestMain:
    """Tests for the ingestion entrypoint."""

    @patch("main.run_geo")
    @patch("main.run_sirene")
    @patch("main.run_france_travail")
    def test_calls_all_sources_in_order(
        self,
        mock_ft: MagicMock,
        mock_sirene: MagicMock,
        mock_geo: MagicMock,
    ) -> None:
        """main() calls all three source run() functions."""
        main()

        mock_ft.assert_called_once()
        mock_sirene.assert_called_once()
        mock_geo.assert_called_once()

    @patch("main.sys.exit")
    @patch("main.run_france_travail", side_effect=RuntimeError("OAuth2 expired"))
    def test_exits_on_failure(
        self,
        mock_ft: MagicMock,
        mock_exit: MagicMock,
    ) -> None:
        """main() logs the error and exits with code 1 on failure."""
        main()

        mock_exit.assert_called_once_with(1)
