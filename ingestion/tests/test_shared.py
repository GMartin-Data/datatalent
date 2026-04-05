"""Tests for ingestion.shared (logging, gcs, bigquery)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import structlog.testing
from google.cloud import bigquery
from shared.bigquery import _infer_source_format, load_gcs_to_bq
from shared.gcs import get_most_recent_blob_date, upload_to_gcs
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

        assert uri.startswith("gs://datatalent-glaq-2-raw/france_travail/")
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
            "gs://datatalent-glaq-2-raw/geo/2026-03-11/regions.json",
            "raw",
            "geo_regions",
        )

        # Verify load_table_from_uri was called with the right URI and table.
        call_args = mock_client.load_table_from_uri.call_args
        assert (
            call_args[0][0] == "gs://datatalent-glaq-2-raw/geo/2026-03-11/regions.json"
        )
        assert call_args[0][1] == "raw.geo_regions"

        # Verify the job config.
        job_config = call_args[1]["job_config"]
        assert job_config.autodetect is True
        assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE

        # Verify job.result() was called (blocking wait).
        mock_job.result.assert_called_once()

    @patch("shared.bigquery.bigquery.Client")
    def test_skips_ingestion_date_stamp_when_partitioned(
        self, mock_client_cls: MagicMock
    ) -> None:
        """load_gcs_to_bq skips ALTER + UPDATE when time_partitioning is set."""
        mock_client = mock_client_cls.return_value

        load_gcs_to_bq(
            "gs://bucket/france_travail/2026-04-05/offres.json",
            "raw",
            "france_travail",
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(field="_ingestion_date"),
        )

        # client.query() should NOT be called (no ALTER, no UPDATE)
        mock_client.query.assert_not_called()

    @patch("shared.bigquery.bigquery.Client")
    def test_write_append_disposition(self, mock_client_cls: MagicMock) -> None:
        """load_gcs_to_bq passes WRITE_APPEND to the job config when specified."""
        mock_client = mock_client_cls.return_value

        load_gcs_to_bq(
            "gs://bucket/adzuna/2026-03-30/adzuna.jsonl",
            "raw",
            "adzuna",
            write_disposition="WRITE_APPEND",
        )

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND

    def test_invalid_write_disposition_raises(self) -> None:
        """load_gcs_to_bq raises ValueError for unsupported write_disposition."""
        with pytest.raises(ValueError, match="Unsupported write disposition"):
            load_gcs_to_bq(
                "gs://bucket/data.json",
                "raw",
                "table",
                write_disposition="WRITE_EMPTY",
            )

    @patch("shared.bigquery.bigquery.Client")
    def test_explicit_schema_disables_autodetect(
        self, mock_client_cls: MagicMock
    ) -> None:
        """load_gcs_to_bq sets autodetect=False and uses provided schema."""
        mock_client = mock_client_cls.return_value

        schema = [
            bigquery.SchemaField("nom", "STRING"),
            bigquery.SchemaField("code", "STRING"),
        ]

        load_gcs_to_bq(
            "gs://bucket/geo/2026-04-04/regions.json",
            "raw",
            "geo_regions",
            schema=schema,
        )

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.autodetect is False
        assert job_config.schema == schema

    @patch("shared.bigquery.bigquery.Client")
    def test_no_schema_enables_autodetect(self, mock_client_cls: MagicMock) -> None:
        """load_gcs_to_bq defaults to autodetect=True when no schema provided."""
        mock_client = mock_client_cls.return_value

        load_gcs_to_bq(
            "gs://bucket/geo/2026-04-04/regions.json",
            "raw",
            "geo_regions",
        )

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.autodetect is True
        assert job_config.schema is None

    @patch("shared.bigquery.bigquery.Client")
    def test_time_partitioning_passed_to_job_config(
        self, mock_client_cls: MagicMock
    ) -> None:
        """load_gcs_to_bq sets time_partitioning on the job config when provided."""
        mock_client = mock_client_cls.return_value
        partitioning = bigquery.TimePartitioning(field="_ingestion_date")

        load_gcs_to_bq(
            "gs://bucket/france_travail/2026-04-05/offres.json",
            "raw",
            "france_travail",
            write_disposition="WRITE_APPEND",
            time_partitioning=partitioning,
        )

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.time_partitioning == partitioning

    @patch("shared.bigquery.bigquery.Client")
    def test_clustering_fields_passed_to_job_config(
        self, mock_client_cls: MagicMock
    ) -> None:
        """load_gcs_to_bq sets clustering_fields on the job config when provided."""
        mock_client = mock_client_cls.return_value
        clustering = ["code_commune", "categorie_metier"]

        load_gcs_to_bq(
            "gs://bucket/france_travail/2026-04-05/offres.json",
            "raw",
            "france_travail",
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(field="_ingestion_date"),
            clustering_fields=clustering,
        )

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.clustering_fields == clustering


class TestGetMostRecentBlobDate:
    """Tests for get_most_recent_blob_date()."""

    @patch("shared.gcs.storage.Client")
    def test_returns_none_when_no_blobs(self, mock_client_cls: MagicMock) -> None:
        mock_client_cls.return_value.list_blobs.return_value = []

        result = get_most_recent_blob_date("sirene")

        assert result is None
        mock_client_cls.return_value.list_blobs.assert_called_once_with(
            "datatalent-glaq-2-raw", prefix="sirene/"
        )

    @patch("shared.gcs.storage.Client")
    def test_returns_most_recent_date(self, mock_client_cls: MagicMock) -> None:
        from datetime import UTC, datetime

        old_blob = MagicMock()
        old_blob.updated = datetime(2026, 2, 1, tzinfo=UTC)
        old_blob.name = "sirene/2026-02-01/stock.parquet"

        recent_blob = MagicMock()
        recent_blob.updated = datetime(2026, 3, 15, tzinfo=UTC)
        recent_blob.name = "sirene/2026-03-15/stock.parquet"

        mock_client_cls.return_value.list_blobs.return_value = [old_blob, recent_blob]

        result = get_most_recent_blob_date("sirene")

        assert result == datetime(2026, 3, 15, tzinfo=UTC)
