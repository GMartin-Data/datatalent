# ingestion/tests/test_france_travail_auth.py
import time
import pytest
from unittest.mock import patch, MagicMock
from france_travail import auth


def _reset_cache():
    """Remet le cache à zéro entre chaque test."""
    auth._token_cache["access_token"] = None
    auth._token_cache["expires_at"] = 0


class TestGetToken:
    """Tests pour get_token() — cache, expiration et renouvellement."""

    def test_appel_api(self, mock_post):
        """Vérifie qu'un appel HTTP est bien fait quand le cache est vide."""
        _reset_cache()
        token = auth.get_token()
        assert token == "fake_token_123"
        mock_post.assert_called_once()

    def test_remplit_cache(self, mock_post):
        """Vérifie que le token est mis en cache après le premier appel."""
        _reset_cache()
        auth.get_token()
        assert auth._token_cache["access_token"] == "fake_token_123"
        assert auth._token_cache["expires_at"] > time.time()

    def test_utilise_cache(self, mock_post):
        """Vérifie qu'un deuxième appel ne refait pas de requête HTTP si le token est valide."""
        _reset_cache()
        auth.get_token()
        auth.get_token()
        assert mock_post.call_count == 1

    def test_renouvelle_si_expire(self, mock_post):
        """Vérifie qu'un nouveau token est demandé si le token en cache est expiré."""
        _reset_cache()
        auth._token_cache["access_token"] = "old_token"
        auth._token_cache["expires_at"] = time.time() - 10
        token = auth.get_token()
        assert token == "fake_token_123"
        assert mock_post.call_count == 1

    def test_renouvelle_dans_marge(self, mock_post):
        """Vérifie le renouvellement anticipé (marge de 60s avant expiration)."""
        _reset_cache()
        auth._token_cache["access_token"] = "almost_expired_token"
        auth._token_cache["expires_at"] = time.time() + 30
        token = auth.get_token()
        assert token == "fake_token_123"
        assert mock_post.call_count == 1


class TestInvalidateToken:
    """Tests pour invalidate_token() — forçage du renouvellement."""

    def test_force_renouvellement(self, mock_post):
        """Vérifie qu'après invalidate_token(), le prochain get_token() refait un appel HTTP."""
        _reset_cache()
        auth.get_token()
        auth.invalidate_token()
        auth.get_token()
        assert mock_post.call_count == 2


@pytest.fixture(autouse=True)
def mock_post():
    """Mock global de requests.post pour tous les tests de ce fichier."""
    with patch("france_travail.auth.httpx.post") as mock:
        response = MagicMock()
        response.json.return_value = {
            "access_token": "fake_token_123",
            "expires_in": 1499
        }
        response.raise_for_status = MagicMock()
        mock.return_value = response
        yield mock