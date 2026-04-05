from unittest.mock import ANY, MagicMock, call, patch

import httpx
import pytest
from geo.ingest import COMMUNES_SCHEMA, DEPARTEMENTS_SCHEMA, REGIONS_SCHEMA

import ingestion.geo.ingest as ingest_module

false_response = [
    {"nom": "Hauts-de-France", "code": "32", "_score": 1},
    {"nom": "Hauts-de-Seine", "code": "92", "_score": 1},
]


@patch("ingestion.geo.ingest.logger")
@patch("ingestion.geo.ingest.httpx.get")
@patch("ingestion.geo.ingest.upload_to_gcs")
@patch("ingestion.geo.ingest.load_gcs_to_bq")
def test_run_happy_path(mock_load, mock_upload, mock_get, mock_logger):
    """
    Teste l'exécution complète du flux d'ingestion géographique (Happy Path).

    Vérifie que lorsque l'API répond correctement, le script effectue bien :
    - Les requêtes HTTP pour chaque ressource.
    - L'upload des données vers le dossier 'geo' sur Google Cloud Storage.
    - Le chargement dans BigQuery vers le dataset 'raw' avec le bon nom de table.
    """
    mock_response = MagicMock()
    mock_response.json.return_value = false_response
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    mock_upload.return_value = "gs://fake/path.jsonl"

    ingest_module.run()

    assert "_ingestion_date" in false_response[0]

    assert mock_get.call_count == 3
    assert mock_logger.info.called

    mock_upload.assert_has_calls(
        [call(ANY, "geo"), call(ANY, "geo"), call(ANY, "geo")], any_order=True
    )

    mock_load.assert_has_calls(
        [
            call("gs://fake/path.jsonl", "raw", "geo_regions", schema=REGIONS_SCHEMA),
            call(
                "gs://fake/path.jsonl",
                "raw",
                "geo_departements",
                schema=DEPARTEMENTS_SCHEMA,
            ),
            call("gs://fake/path.jsonl", "raw", "geo_communes", schema=COMMUNES_SCHEMA),
        ],
        any_order=True,
    )


@patch("ingestion.geo.ingest.httpx.get")
def test_fetch_geo_data_empty_response(mock_get):
    """
    Simule une réponse d'API au format JSON mais vide.

    Vérifie que la fonction retourne une liste vide sans provoquer d'erreur interne.
    """
    mock_response = MagicMock()
    mock_response.json.return_value = []
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    resultat = ingest_module.fetch_geo_data("regions")
    assert resultat == []


@patch("ingestion.geo.ingest.httpx.get")
def test_fetch_geo_data_http_5xx(mock_get):
    """
    Simule une erreur interne du serveur distant (Code HTTP 500).

    Vérifie que la fonction lève explicitement une exception httpx.HTTPStatusError.
    """
    mock_response = MagicMock()
    mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
        "500 Internal Server Error", request=MagicMock(), response=mock_response
    )

    with pytest.raises(httpx.HTTPStatusError):
        ingest_module.fetch_geo_data("regions")


@patch("ingestion.geo.ingest.httpx.get")
def test_fetch_geo_data_network_error(mock_get):
    """
    Simule une impossibilité de joindre le serveur de l'API (Timeout/Coupure réseau).

    Vérifie que la fonction lève explicitement une exception httpx.ConnectTimeout.
    """
    mock_get.side_effect = httpx.ConnectTimeout("Timeout")

    with pytest.raises(httpx.ConnectTimeout):
        ingest_module.fetch_geo_data("regions")
