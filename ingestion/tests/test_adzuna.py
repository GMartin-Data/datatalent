"""Tests pour ingestion.adzuna.client."""

from unittest.mock import MagicMock, patch

import httpx
import pytest
from adzuna.client import fetch_all_offers


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

    @patch("ingestion.adzuna.client.SLEEP_BETWEEN_REQUESTS", 0)
    @patch("ingestion.adzuna.client.httpx.Client")
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

    @patch("ingestion.adzuna.client.SLEEP_BETWEEN_REQUESTS", 0)
    @patch("ingestion.adzuna.client.httpx.Client")
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

    @patch("ingestion.adzuna.client.SLEEP_BETWEEN_REQUESTS", 0)
    @patch("ingestion.adzuna.client.httpx.Client")
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

    @patch("ingestion.adzuna.client.SLEEP_BETWEEN_REQUESTS", 0)
    @patch("ingestion.adzuna.client.httpx.Client")
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
