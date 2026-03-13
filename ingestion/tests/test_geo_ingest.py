from unittest.mock import MagicMock, patch

import ingestion.geo.ingest as ingest_module

false_response = [
    {"nom": "Hauts-de-France", "code": "32", "_score": 1},
    {"nom": "Hauts-de-Seine", "code": "92", "_score": 1},
]


def test_fetch_geo_data():
    with patch("ingestion.geo.ingest.httpx.get") as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = false_response
        mock_get.return_value = mock_response
        resultat = ingest_module.fetch_geo_data("regions")
        assert resultat == false_response


def test_run():
    with (
        patch("ingestion.geo.ingest.logger") as mock_logger,
        patch("ingestion.geo.ingest.fetch_geo_data") as mock_fetch,
        patch("ingestion.geo.ingest.upload_to_gcs") as mock_upload,
        patch("ingestion.geo.ingest.load_gcs_to_bq") as mock_load,
        patch(
            "ingestion.geo.ingest.RESOURCES",
            {"regions": "r", "departements": "d", "communes": "c"},
        ),
    ):
        mock_fetch.return_value = false_response

        def side_effect_check(local_path, destination):
            with open(local_path) as f:
                lines = f.read().strip().split("\n")
            assert len(lines) == 2
            return "gs://fake/path.jsonl"

        mock_upload.side_effect = side_effect_check

        ingest_module.run()

        assert mock_fetch.call_count == 3
        assert mock_upload.call_count == 3
        assert mock_load.call_count == 3

        assert mock_logger.info.called
