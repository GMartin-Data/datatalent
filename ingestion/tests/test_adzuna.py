"""Tests pour adzuna.client et adzuna.ingest."""

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest
from adzuna.client import fetch_all_offers
from adzuna.ingest import ADZUNA_SCHEMA, _extract_value, _map_offer, run
from google.cloud import bigquery


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> MagicMock:
    """Crée un mock httpx.Response."""
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.json.return_value = json_data or {}
    response.raise_for_status = MagicMock()
    if status_code >= 400:
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=response,
        )
    return response


class TestFetchPage:
    """Tests pour _fetch_page via fetch_all_offers."""

    @patch("adzuna.client.SLEEP_BETWEEN_REQUESTS", 0)
    @patch("adzuna.client.httpx.Client")
    def test_pagination_two_pages(self, mock_client_cls):
        """Deux pages : la première pleine (50), la seconde partielle (10) → arrêt."""
        page1 = _mock_response(
            json_data={
                "results": [{"id": str(i)} for i in range(50)],
                "count": 60,
            }
        )
        page2 = _mock_response(
            json_data={
                "results": [{"id": str(i)} for i in range(50, 60)],
                "count": 60,
            }
        )

        mock_client = MagicMock()
        mock_client.get.side_effect = [page1, page2]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        offers = fetch_all_offers("fake_id", "fake_key")

        assert len(offers) == 60
        assert mock_client.get.call_count == 2

    @patch("adzuna.client.SLEEP_BETWEEN_REQUESTS", 0)
    @patch("adzuna.client.httpx.Client")
    def test_single_page(self, mock_client_cls):
        """Moins de 50 résultats → une seule page, pas de sleep."""
        page1 = _mock_response(
            json_data={
                "results": [{"id": "1"}, {"id": "2"}],
                "count": 2,
            }
        )

        mock_client = MagicMock()
        mock_client.get.side_effect = [page1]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        offers = fetch_all_offers("fake_id", "fake_key")

        assert len(offers) == 2
        assert mock_client.get.call_count == 1


class TestRetry:
    """Tests pour le mécanisme de retry."""

    @patch("adzuna.client.SLEEP_BETWEEN_REQUESTS", 0)
    @patch("adzuna.client.httpx.Client")
    def test_retry_then_success(self, mock_client_cls):
        """429 au premier appel, succès au deuxième → retry transparent."""
        response_429 = _mock_response(status_code=429)
        response_ok = _mock_response(
            json_data={
                "results": [{"id": "1"}],
                "count": 1,
            }
        )

        mock_client = MagicMock()
        mock_client.get.side_effect = [response_429, response_ok]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        offers = fetch_all_offers("fake_id", "fake_key")

        assert len(offers) == 1
        assert mock_client.get.call_count == 2

    @patch("adzuna.client.SLEEP_BETWEEN_REQUESTS", 0)
    @patch("adzuna.client.httpx.Client")
    def test_fatal_error_no_retry(self, mock_client_cls):
        """400 Bad Request → HTTPStatusError immédiate, pas de retry."""
        response_400 = _mock_response(status_code=400)

        mock_client = MagicMock()
        mock_client.get.side_effect = [response_400]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.HTTPStatusError):
            fetch_all_offers("fake_id", "fake_key")

        assert mock_client.get.call_count == 1


class TestExtractValue:
    """Tests pour _extract_value."""

    def test_flat_key(self):
        """Clé simple, premier niveau."""
        assert _extract_value({"id": "123"}, "id") == "123"

    def test_nested_key(self):
        """Clé imbriquée via notation pointée."""
        offer = {"company": {"display_name": "Acme"}}
        assert _extract_value(offer, "company.display_name") == "Acme"

    def test_missing_intermediate(self):
        """Niveau intermédiaire absent → None."""
        assert _extract_value({}, "company.display_name") is None

    def test_missing_leaf(self):
        """Niveau final absent → None."""
        offer = {"company": {}}
        assert _extract_value(offer, "company.display_name") is None

    def test_intermediate_not_dict(self):
        """Niveau intermédiaire n'est pas un dict → None."""
        offer = {"company": "string_value"}
        assert _extract_value(offer, "company.display_name") is None

    def test_array_value(self):
        """Valeur de type array retournée telle quelle."""
        offer = {"location": {"area": ["France", "Île-de-France", "Paris"]}}
        assert _extract_value(offer, "location.area") == [
            "France",
            "Île-de-France",
            "Paris",
        ]


class TestMapOffer:
    """Tests pour _map_offer."""

    def test_full_offer(self):
        """Offre complète — tous les champs mappés."""
        offer = {
            "id": "42",
            "title": "Data Engineer",
            "company": {"display_name": "Acme"},
            "location": {
                "display_name": "Paris",
                "area": ["France", "Île-de-France"],
            },
            "latitude": 48.85,
            "longitude": 2.35,
            "salary_min": 45000,
            "salary_max": 55000,
            "salary_is_predicted": "0",
            "description": "Poste data engineer...",
            "redirect_url": "https://adzuna.fr/land/ad/123",
            "category": {"tag": "it-jobs", "label": "IT Jobs"},
            "contract_type": "permanent",
            "contract_time": "full_time",
            "created": "2026-03-20T07:53:25Z",
        }
        mapped = _map_offer(offer)

        assert mapped["offre_id"] == "42"
        assert mapped["titre"] == "Data Engineer"
        assert mapped["entreprise_nom"] == "Acme"
        assert mapped["localisation_libelle"] == "Paris"
        assert mapped["localisation_area"] == ["France", "Île-de-France"]
        assert mapped["latitude"] == 48.85
        assert mapped["salaire_min"] == 45000
        assert mapped["salaire_est_estime"] == "0"
        assert mapped["categorie_tag"] == "it-jobs"
        assert mapped["type_contrat"] == "permanent"
        assert mapped["date_creation"] == "2026-03-20T07:53:25Z"

    def test_sparse_offer(self):
        """Offre minimale — champs absents mappés à None."""
        offer = {
            "id": "99",
            "title": "Data Engineer Junior",
            "created": "2026-03-25T10:00:00Z",
        }
        mapped = _map_offer(offer)

        assert mapped["offre_id"] == "99"
        assert mapped["titre"] == "Data Engineer Junior"
        assert mapped["entreprise_nom"] is None
        assert mapped["salaire_min"] is None
        assert mapped["latitude"] is None
        assert mapped["categorie_tag"] is None

    def test_excluded_fields(self):
        """Les champs __CLASS__ et adref ne sont pas dans le résultat."""
        offer = {
            "id": "1",
            "title": "DE",
            "__CLASS__": "Adzuna::API::Response::Job",
            "adref": "abc123",
            "created": "2026-03-25T10:00:00Z",
        }
        mapped = _map_offer(offer)

        assert "__CLASS__" not in mapped
        assert "adref" not in mapped
        assert "__class__" not in mapped.values()
        assert "abc123" not in mapped.values()


class TestRun:
    """Tests d'intégration pour run()."""

    @patch("adzuna.ingest.load_gcs_to_bq")
    @patch(
        "adzuna.ingest.upload_to_gcs",
        return_value="gs://datatalent-raw/adzuna/2026-03-30/adzuna.jsonl",
    )
    @patch("adzuna.ingest.fetch_all_offers")
    @patch("adzuna.ingest.get_credentials", return_value=("fake_id", "fake_key"))
    def test_run_happy_path(self, _mock_creds, mock_fetch, mock_gcs, mock_bq, tmp_path):
        """Run complet : credentials → fetch → JSONL → GCS → BQ."""
        mock_fetch.return_value = [
            {
                "id": "1",
                "title": "Data Engineer",
                "company": {"display_name": "Acme"},
                "location": {"display_name": "Paris", "area": ["France"]},
                "latitude": 48.85,
                "longitude": 2.35,
                "description": "Poste DE",
                "redirect_url": "https://adzuna.fr/land/ad/1",
                "category": {"tag": "it-jobs", "label": "IT Jobs"},
                "created": "2026-03-20T07:53:25Z",
            },
        ]

        jsonl_path = str(tmp_path / "adzuna.jsonl")
        with patch("adzuna.ingest.LOCAL_JSONL_PATH", jsonl_path):
            run()

        # Vérification JSONL écrit
        with open(jsonl_path, encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["offre_id"] == "1"
        assert record["entreprise_nom"] == "Acme"
        assert record["salaire_min"] is None
        assert "_ingestion_date" in record

        # Vérification appels shared/
        mock_gcs.assert_called_once_with(jsonl_path, "adzuna")
        mock_bq.assert_called_once_with(
            "gs://datatalent-raw/adzuna/2026-03-30/adzuna.jsonl",
            "raw",
            "adzuna",
            write_disposition="WRITE_APPEND",
            schema=ADZUNA_SCHEMA,
            time_partitioning=bigquery.TimePartitioning(field="_ingestion_date"),
        )

    @patch("adzuna.ingest.load_gcs_to_bq")
    @patch("adzuna.ingest.upload_to_gcs", return_value="gs://fake")
    @patch("adzuna.ingest.fetch_all_offers", return_value=[])
    @patch("adzuna.ingest.get_credentials", return_value=("fake_id", "fake_key"))
    def test_run_empty_results(
        self, _mock_creds, _mock_fetch, mock_gcs, mock_bq, tmp_path
    ):
        """Aucune offre → JSONL vide, GCS et BQ appelés quand même."""
        jsonl_path = str(tmp_path / "adzuna.jsonl")
        with patch("adzuna.ingest.LOCAL_JSONL_PATH", jsonl_path):
            run()

        with open(jsonl_path, encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) == 0

        mock_gcs.assert_called_once()
        mock_bq.assert_called_once()
