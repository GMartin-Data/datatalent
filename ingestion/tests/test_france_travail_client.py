"""Tests pour FranceTravailClient — auth OAuth2 + fetch offres paginé."""

import time
from unittest.mock import MagicMock, patch

import pytest
import tenacity
from france_travail.client import FranceTravailClient, RetryableAPIError

# --- Helpers ---


def _make_token_response(token: str = "fake_token_123", expires_in: int = 1499):
    """Fabrique une fausse réponse OAuth2."""
    mock = MagicMock()
    mock.json.return_value = {"access_token": token, "expires_in": expires_in}
    mock.raise_for_status = MagicMock()
    return mock


def _make_offres_response(resultats: list, total: int, start: int, end: int):
    """Fabrique une fausse réponse de l'API offres."""
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = {"resultats": resultats}
    mock.headers = {"Content-Range": f"offres {start}-{end}/{total}"}
    mock.raise_for_status = MagicMock()
    return mock


def _make_error_response(status_code: int):
    """Fabrique une fausse réponse d'erreur (429, 401, etc.)."""
    mock = MagicMock()
    mock.status_code = status_code
    mock.raise_for_status = MagicMock()
    return mock


# --- Auth ---


class TestGetToken:
    """Tests pour _get_token() — cache, expiration et renouvellement."""

    def test_appel_api(self, mock_http):
        """Vérifie qu'un appel HTTP est bien fait quand le cache est vide."""
        mock_http.post.return_value = _make_token_response()

        client = FranceTravailClient("id", "secret")
        token = client._get_token()

        assert token == "fake_token_123"
        mock_http.post.assert_called_once()

    def test_remplit_cache(self, mock_http):
        """Vérifie que le token est mis en cache après le premier appel."""
        mock_http.post.return_value = _make_token_response()

        client = FranceTravailClient("id", "secret")
        client._get_token()

        assert client._token_cache["access_token"] == "fake_token_123"
        assert client._token_cache["expires_at"] > time.time()

    def test_utilise_cache(self, mock_http):
        """Vérifie qu'un 2e appel ne refait pas de requête si le token est valide."""
        mock_http.post.return_value = _make_token_response()

        client = FranceTravailClient("id", "secret")
        client._get_token()
        client._get_token()

        assert mock_http.post.call_count == 1

    def test_renouvelle_si_expire(self, mock_http):
        """Vérifie qu'un nouveau token est demandé si le token en cache est expiré."""
        mock_http.post.return_value = _make_token_response()

        client = FranceTravailClient("id", "secret")
        client._token_cache["access_token"] = "old_token"
        client._token_cache["expires_at"] = time.time() - 10

        token = client._get_token()

        assert token == "fake_token_123"
        mock_http.post.assert_called_once()

    def test_renouvelle_dans_marge(self, mock_http):
        """Vérifie le renouvellement anticipé (marge de 60s avant expiration)."""
        mock_http.post.return_value = _make_token_response()

        client = FranceTravailClient("id", "secret")
        client._token_cache["access_token"] = "almost_expired_token"
        client._token_cache["expires_at"] = time.time() + 30

        token = client._get_token()

        assert token == "fake_token_123"
        mock_http.post.assert_called_once()


class TestInvalidateToken:
    """Tests pour _invalidate_token() — forçage du renouvellement."""

    def test_force_renouvellement(self, mock_http):
        """Vérifie qu'après ivalidation, le prochain appel redemande un token."""
        mock_http.post.return_value = _make_token_response()

        client = FranceTravailClient("id", "secret")
        client._get_token()
        client._invalidate_token()
        client._get_token()

        assert mock_http.post.call_count == 2


# --- Fetch ---


class TestFetchOffres:
    """Tests pour fetch_offres() — pagination."""

    def test_un_seul_batch(self, mock_http):
        """Vérifie qu'un seul appel est fait si les offres tiennent dans un batch."""
        mock_http.post.return_value = _make_token_response()
        offres_data = [{"id": str(i)} for i in range(10)]
        mock_http.get.return_value = _make_offres_response(
            offres_data, total=10, start=0, end=9
        )

        client = FranceTravailClient("id", "secret")
        with patch("france_travail.client.time.sleep"):
            result = client.fetch_offres("M1805", "75")

        assert len(result) == 10
        assert mock_http.get.call_count == 1

    def test_plusieurs_batchs(self, mock_http):
        """Vérifie que la pagination boucle correctement sur plusieurs batchs."""
        mock_http.post.return_value = _make_token_response()
        batch1 = [{"id": str(i)} for i in range(150)]
        batch2 = [{"id": str(i)} for i in range(50)]
        mock_http.get.side_effect = [
            _make_offres_response(batch1, total=200, start=0, end=149),
            _make_offres_response(batch2, total=200, start=150, end=199),
        ]

        client = FranceTravailClient("id", "secret")
        with patch("france_travail.client.time.sleep"):
            result = client.fetch_offres("M1805", "75")

        assert len(result) == 200
        assert mock_http.get.call_count == 2

    def test_stoppe_si_vide(self, mock_http):
        """Vérifie que la boucle s'arrête si l'API retourne une liste vide."""
        mock_http.post.return_value = _make_token_response()
        mock_http.get.return_value = _make_offres_response([], total=0, start=0, end=0)

        client = FranceTravailClient("id", "secret")
        with patch("france_travail.client.time.sleep"):
            result = client.fetch_offres("M1805", "75")

        assert result == []
        assert mock_http.get.call_count == 1

    def test_204_retourne_liste_vide(self, mock_http):
        """Vérifie que le 204 No Content retourne une liste vide sans crash."""
        mock_http.post.return_value = _make_token_response()
        mock_204 = MagicMock()
        mock_204.status_code = 204
        mock_http.get.return_value = mock_204

        client = FranceTravailClient("id", "secret")
        with patch("france_travail.client.time.sleep"):
            result = client.fetch_offres("M1805", "75")

        assert result == []
        assert mock_http.get.call_count == 1

    def test_sleep_entre_requetes(self, mock_http):
        """Vérifie que le throttle est appliqué entre chaque batch."""
        mock_http.post.return_value = _make_token_response()
        batch1 = [{"id": str(i)} for i in range(150)]
        batch2 = [{"id": str(i)} for i in range(50)]
        mock_http.get.side_effect = [
            _make_offres_response(batch1, total=200, start=0, end=149),
            _make_offres_response(batch2, total=200, start=150, end=199),
        ]

        client = FranceTravailClient("id", "secret")
        with patch("france_travail.client.time.sleep") as mock_sleep:
            client.fetch_offres("M1805", "75")

        # 2 batchs = 2 appels sleep(0.15)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(0.15)


class TestFetchBatchRetry:
    def test_retry_sur_429(self, mock_http):
        """Vérifie qu'un 429 suivi d'un succès retourne les offres."""
        mock_http.post.return_value = _make_token_response()
        mock_http.get.side_effect = [
            _make_error_response(429),
            _make_offres_response([{"id": "1"}], total=1, start=0, end=0),
        ]

        client = FranceTravailClient("id", "secret")
        # Désactive le wait tenacity + le throttle pour le test
        client._request.retry.wait = tenacity.wait_none()  # type: ignore[attr-defined]
        with patch("france_travail.client.time.sleep"):
            result = client.fetch_offres("M1805", "75")

        assert len(result) == 1
        assert result[0]["id"] == "1"
        # 2 appels GET : le 429 + le succès
        assert mock_http.get.call_count == 2

    def test_retry_sur_500(self, mock_http):
        """Vérifie qu'un 500 suivi d'un succès retourne les offres."""
        mock_http.post.return_value = _make_token_response()
        mock_http.get.side_effect = [
            _make_error_response(500),
            _make_offres_response([{"id": "1"}], total=1, start=0, end=0),
        ]

        client = FranceTravailClient("id", "secret")
        client._request.retry.wait = tenacity.wait_none()  # type: ignore[attr-defined]
        with patch("france_travail.client.time.sleep"):
            result = client.fetch_offres("M1805", "75")

        assert len(result) == 1
        assert mock_http.get.call_count == 2

    def test_retry_sur_503(self, mock_http):
        """Vérifie qu'un 503 suivi d'un succès retourne les offres."""
        mock_http.post.return_value = _make_token_response()
        mock_http.get.side_effect = [
            _make_error_response(503),
            _make_offres_response([{"id": "1"}], total=1, start=0, end=0),
        ]

        client = FranceTravailClient("id", "secret")
        client._request.retry.wait = tenacity.wait_none()  # type: ignore[attr-defined]
        with patch("france_travail.client.time.sleep"):
            result = client.fetch_offres("M1805", "75")

        assert len(result) == 1
        assert mock_http.get.call_count == 2

    def test_echoue_apres_max_retries(self, mock_http):
        """Vérifie que RetryableAPIError est levée si le 429 persiste."""
        mock_http.post.return_value = _make_token_response()
        mock_http.get.return_value = _make_error_response(429)

        client = FranceTravailClient("id", "secret")
        client._request.retry.wait = tenacity.wait_none()  # type: ignore[attr-defined]
        with patch("france_travail.client.time.sleep"):
            with pytest.raises(RetryableAPIError):
                client.fetch_offres("M1805", "75")

        # 5 tentatives (stop_after_attempt(5))
        assert mock_http.get.call_count == 5


class TestFetchBatchTokenExpire:
    """Tests pour la gestion du token expiré (401)."""

    def test_retry_sur_401(self, mock_http):
        """Vérifie que sur un 401, le token est invalidé et la requête réessayée."""
        mock_http.post.return_value = _make_token_response()
        mock_http.get.side_effect = [
            _make_error_response(401),
            _make_offres_response([{"id": "1"}], total=1, start=0, end=0),
        ]

        client = FranceTravailClient("id", "secret")
        with patch("france_travail.client.time.sleep"):
            result = client.fetch_offres("M1805", "75")

        # Le token a été invalidé puis re-demandé : 2 appels POST au total
        assert mock_http.post.call_count == 2
        assert len(result) == 1


# --- Parse ---


class TestParseTotal:
    """Tests pour _parse_total() — parsing du header Content-Range."""

    def test_nominal(self):
        """Vérifie le parsing dans le cas standard."""
        assert FranceTravailClient._parse_total("offres 0-149/523") == 523

    def test_header_absent(self):
        """Vérifie que _parse_total retourne 0 si le header est absent."""
        assert FranceTravailClient._parse_total("") == 0

    def test_header_malforme(self):
        """Vérifie que _parse_total ne plante pas sur un header inattendu."""
        assert FranceTravailClient._parse_total("unexpected_format") == 0


# --- Fixtures ---


@pytest.fixture(autouse=True)
def mock_http():
    """Mock httpx.Client pour contrôler les appels HTTP."""
    with patch("france_travail.client.httpx.Client") as MockClient:
        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        yield mock_instance
