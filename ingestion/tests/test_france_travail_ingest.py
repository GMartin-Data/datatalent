"""Tests pour les fonctions d'ingestion France Travail."""

import json
import os
from unittest.mock import patch

from france_travail.ingest import deduplicate_offres, run, write_jsonl
from google.cloud import bigquery


class TestDeduplicateOffres:
    def test_no_duplicates(self):
        offres = [{"id": "A", "titre": "X"}, {"id": "B", "titre": "Y"}]
        result = deduplicate_offres(offres)
        assert len(result) == 2

    def test_removes_duplicates(self):
        offres = [
            {"id": "A", "titre": "v1"},
            {"id": "B", "titre": "Y"},
            {"id": "A", "titre": "v2"},
        ]
        result = deduplicate_offres(offres)
        assert len(result) == 2

    def test_last_wins(self):
        offres = [
            {"id": "A", "titre": "v1"},
            {"id": "A", "titre": "v2"},
        ]
        result = deduplicate_offres(offres)
        assert result[0]["titre"] == "v2"

    def test_empty_list(self):
        assert deduplicate_offres([]) == []


class TestWriteJsonl:
    def test_writes_correct_line_count(self, tmp_path):
        offres = [{"id": "A"}, {"id": "B"}, {"id": "C"}]
        file_path = str(tmp_path / "test.jsonl")
        write_jsonl(offres, file_path)

        with open(file_path) as f:
            lines = f.readlines()
        assert len(lines) == 3

    def test_each_line_is_valid_json(self, tmp_path):
        offres = [{"id": "A", "nom": "test"}, {"id": "B", "nom": "autre"}]
        file_path = str(tmp_path / "test.jsonl")
        write_jsonl(offres, file_path)

        with open(file_path) as f:
            for line in f:
                parsed = json.loads(line)
                assert "id" in parsed

    def test_handles_unicode(self, tmp_path):
        offres = [{"id": "A", "titre": "Ingénieur données"}]
        file_path = str(tmp_path / "test.jsonl")
        write_jsonl(offres, file_path)

        with open(file_path, encoding="utf-8") as f:
            parsed = json.loads(f.readline())
        assert parsed["titre"] == "Ingénieur données"

    def test_empty_list_creates_empty_file(self, tmp_path):
        file_path = str(tmp_path / "test.jsonl")
        write_jsonl([], file_path)
        assert os.path.getsize(file_path) == 0


class TestRun:
    @patch("france_travail.ingest.load_gcs_to_bq")
    @patch(
        "france_travail.ingest.upload_to_gcs",
        return_value="gs://datatalent-glaq-2-raw/france_travail/2026-04-03/file.jsonl",
    )
    @patch("france_travail.ingest.FranceTravailClient")
    def test_nominal(self, MockClient, mock_gcs, mock_bq, tmp_path, monkeypatch):
        """Flux complet : extract → dédup → JSONL → GCS → BQ."""
        monkeypatch.setenv("FT_CLIENT_ID", "fake_id")
        monkeypatch.setenv("FT_CLIENT_SECRET", "fake_secret")
        monkeypatch.setattr("france_travail.ingest.OUTPUT_DIR", str(tmp_path))
        monkeypatch.setattr("france_travail.ingest.CODES_ROME", ["M1805"])
        monkeypatch.setattr("france_travail.ingest.DEPARTEMENTS", ["75"])

        mock_instance = MockClient.return_value.__enter__.return_value
        mock_instance.fetch_offres.return_value = [
            {"id": "1", "titre": "Dev"},
            {"id": "2", "titre": "Data Engineer"},
        ]

        run()

        mock_instance.fetch_offres.assert_called_once_with("M1805", "75")
        jsonl_files = list(tmp_path.glob("*.jsonl"))
        assert len(jsonl_files) == 1
        assert jsonl_files[0].read_text().count("\n") == 2
        mock_gcs.assert_called_once()
        assert mock_gcs.call_args[0][1] == "france_travail"
        mock_bq.assert_called_once_with(
            "gs://datatalent-glaq-2-raw/france_travail/2026-04-03/file.jsonl",
            "raw",
            "france_travail",
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(field="_ingestion_date"),
        )

    @patch("france_travail.ingest.load_gcs_to_bq")
    @patch(
        "france_travail.ingest.upload_to_gcs",
        return_value="gs://datatalent-glaq-2-raw/france_travail/2026-04-03/file.jsonl",
    )
    @patch("france_travail.ingest.FranceTravailClient")
    def test_zero_offres(self, MockClient, mock_gcs, mock_bq, tmp_path, monkeypatch):
        """Zéro offre : fichier JSONL vide, GCS et BQ appelés quand même."""
        monkeypatch.setenv("FT_CLIENT_ID", "fake_id")
        monkeypatch.setenv("FT_CLIENT_SECRET", "fake_secret")
        monkeypatch.setattr("france_travail.ingest.OUTPUT_DIR", str(tmp_path))
        monkeypatch.setattr("france_travail.ingest.CODES_ROME", ["M1805"])
        monkeypatch.setattr("france_travail.ingest.DEPARTEMENTS", ["75"])

        mock_instance = MockClient.return_value.__enter__.return_value
        mock_instance.fetch_offres.return_value = []

        run()

        jsonl_files = list(tmp_path.glob("*.jsonl"))
        assert len(jsonl_files) == 1
        assert jsonl_files[0].read_text() == ""
        mock_gcs.assert_called_once()
        mock_bq.assert_called_once()

    @patch("france_travail.ingest.load_gcs_to_bq")
    @patch(
        "france_travail.ingest.upload_to_gcs",
        return_value="gs://datatalent-glaq-2-raw/france_travail/2026-04-03/file.jsonl",
    )
    @patch("france_travail.ingest.FranceTravailClient")
    def test_skip_on_http_error(
        self, MockClient, mock_gcs, mock_bq, tmp_path, monkeypatch
    ):
        """Une combinaison ROME×dept échoue en 400, les autres passent."""
        import httpx

        monkeypatch.setenv("FT_CLIENT_ID", "fake_id")
        monkeypatch.setenv("FT_CLIENT_SECRET", "fake_secret")
        monkeypatch.setattr("france_travail.ingest.OUTPUT_DIR", str(tmp_path))
        monkeypatch.setattr("france_travail.ingest.CODES_ROME", ["M1805"])
        monkeypatch.setattr("france_travail.ingest.DEPARTEMENTS", ["75", "13"])

        error_response = httpx.Response(
            status_code=400, request=httpx.Request("GET", "http://test")
        )

        mock_instance = MockClient.return_value.__enter__.return_value
        mock_instance.fetch_offres.side_effect = [
            httpx.HTTPStatusError(
                "Bad Request", request=error_response.request, response=error_response
            ),
            [{"id": "1", "titre": "Data Engineer"}],
        ]

        run()

        assert mock_instance.fetch_offres.call_count == 2
        jsonl_files = list(tmp_path.glob("*.jsonl"))
        assert jsonl_files[0].read_text().count("\n") == 1
        mock_gcs.assert_called_once()
        mock_bq.assert_called_once()

    @patch("france_travail.ingest.load_gcs_to_bq")
    @patch(
        "france_travail.ingest.upload_to_gcs",
        side_effect=Exception("GCS down"),
    )
    @patch("france_travail.ingest.FranceTravailClient")
    def test_gcs_error_stops_pipeline(
        self, MockClient, mock_gcs, mock_bq, tmp_path, monkeypatch
    ):
        """Si l'upload GCS plante, le load BQ ne doit pas être appelé."""
        import pytest

        monkeypatch.setenv("FT_CLIENT_ID", "fake_id")
        monkeypatch.setenv("FT_CLIENT_SECRET", "fake_secret")
        monkeypatch.setattr("france_travail.ingest.OUTPUT_DIR", str(tmp_path))
        monkeypatch.setattr("france_travail.ingest.CODES_ROME", ["M1805"])
        monkeypatch.setattr("france_travail.ingest.DEPARTEMENTS", ["75"])

        mock_instance = MockClient.return_value.__enter__.return_value
        mock_instance.fetch_offres.return_value = [{"id": "1"}]

        with pytest.raises(Exception, match="GCS down"):
            run()

        mock_gcs.assert_called_once()
        mock_bq.assert_not_called()
