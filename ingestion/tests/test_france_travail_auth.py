# ingestion/tests/test_france_travail_auth.py
import time
import pytest
from unittest.mock import patch, MagicMock
from france_travail import auth  


def _reset_cache():
    """Remet le cache à zéro entre chaque test."""
    auth._token_cache["access_token"] = None
    auth._token_cache["expires_at"] = 0


# --- get_token : appel réseau ---

def test_get_token_appel_api(mock_post):
    """
    Vérifie qu'un appel HTTP est bien fait quand le cache est vide.
    Sans ça on ne saurait pas si auth.py contacte réellement France Travail.
    """
    _reset_cache()
    token = auth.get_token()
    assert token == "fake_token_123"
    mock_post.assert_called_once()


def test_get_token_remplit_cache(mock_post):
    """
    Vérifie que le token est bien mis en cache après le premier appel.
    Sans cache, chaque requête déclencherait un appel OAuth2 inutile.
    """
    _reset_cache()
    auth.get_token()
    assert auth._token_cache["access_token"] == "fake_token_123"
    assert auth._token_cache["expires_at"] > time.time()


def test_get_token_utilise_cache(mock_post):
    """
    Vérifie qu'un deuxième appel NE refait PAS de requête HTTP si le token est valide.
    C'est le comportement central du cache.
    """
    _reset_cache()
    auth.get_token()
    auth.get_token()
    assert mock_post.call_count == 1  # un seul appel réseau pour deux get_token()


def test_get_token_renouvelle_si_expire(mock_post):
    """
    Vérifie qu'un nouveau token est demandé si le token en cache est expiré.
    Sans ça, on enverrait un token périmé et obtiendrait des 401.
    """
    _reset_cache()
    auth._token_cache["access_token"] = "old_token"
    auth._token_cache["expires_at"] = time.time() - 10  # expiré depuis 10s
    token = auth.get_token()
    assert token == "fake_token_123"
    assert mock_post.call_count == 1


def test_get_token_renouvelle_dans_marge(mock_post):
    """
    Vérifie le renouvellement anticipé (marge de 60s avant expiration).
    Evite les 401 en pleine boucle d'ingestion si le token expire pendant l'exécution.
    """
    _reset_cache()
    auth._token_cache["access_token"] = "almost_expired_token"
    auth._token_cache["expires_at"] = time.time() + 30  # expire dans 30s < marge 60s
    token = auth.get_token()
    assert token == "fake_token_123"
    assert mock_post.call_count == 1


# --- invalidate_token ---

def test_invalidate_token_force_renouvellement(mock_post):
    """
    Vérifie qu'après invalidate_token(), le prochain get_token() refait un appel HTTP.
    Utilisé lors d'un 401 inattendu côté serveur (token révoqué avant expires_in).
    """
    _reset_cache()
    auth.get_token()
    auth.invalidate_token()
    auth.get_token()
    assert mock_post.call_count == 2


# --- Fixture ---

@pytest.fixture(autouse=True)
def mock_post():
    """Mock global de requests.post pour tous les tests de ce fichier."""
    with patch("france_travail.auth.requests.post") as mock:
        response = MagicMock()
        response.json.return_value = {
            "access_token": "fake_token_123",
            "expires_in": 1499
        }
        response.raise_for_status = MagicMock()
        mock.return_value = response
        yield mock