# ingestion/tests/test_france_travail_offres.py
import pytest
from unittest.mock import patch, MagicMock
from france_travail import offres  # ← remplace le sys.path.insert


def _make_response(resultats: list, total: int, start: int, end: int, status: int = 200):
    """Fabrique un faux objet response requests."""
    mock = MagicMock()
    mock.status_code = status
    mock.json.return_value = {"resultats": resultats}
    mock.headers = {"Content-Range": f"offres {start}-{end}/{total}"}
    mock.raise_for_status = MagicMock()
    return mock


# --- Pagination ---

def test_fetch_all_offres_un_seul_batch(mock_get, mock_token):
    """
    Vérifie qu'un seul appel HTTP est fait si toutes les offres tiennent dans un batch.
    Cas le plus simple : < 150 offres.
    """
    offres_data = [{"id": str(i)} for i in range(10)]
    mock_get.return_value = _make_response(offres_data, total=10, start=0, end=9)

    result = offres.fetch_all_offres("M1805", "75")

    assert len(result) == 10
    assert mock_get.call_count == 1


def test_fetch_all_offres_plusieurs_batchs(mock_get, mock_token):
    """
    Vérifie que la pagination boucle correctement sur plusieurs batchs.
    Cas clé : > 150 offres, on doit tout récupérer sans rien manquer.
    """
    batch1 = [{"id": str(i)} for i in range(150)]
    batch2 = [{"id": str(i)} for i in range(50)]

    mock_get.side_effect = [
        _make_response(batch1, total=200, start=0, end=149),
        _make_response(batch2, total=200, start=150, end=199),
    ]

    result = offres.fetch_all_offres("M1805", "75")

    assert len(result) == 200
    assert mock_get.call_count == 2


def test_fetch_all_offres_stoppe_si_vide(mock_get, mock_token):
    """
    Vérifie que la boucle s'arrête si l'API retourne une liste vide.
    Garde-fou si Content-Range est absent ou incohérent.
    """
    mock_get.return_value = _make_response([], total=0, start=0, end=0)

    result = offres.fetch_all_offres("M1805", "75")

    assert result == []
    assert mock_get.call_count == 1


# --- Backoff 429 ---

def test_fetch_batch_retry_sur_429(mock_get, mock_token):
    """
    Vérifie que sur un 429, le code attend et réessaie.
    Sans ça, un rate limit couperait l'ingestion en plein milieu.
    """
    response_429 = MagicMock()
    response_429.status_code = 429
    response_429.raise_for_status = MagicMock()

    response_ok = _make_response([{"id": "1"}], total=1, start=0, end=0)

    mock_get.side_effect = [response_429, response_ok]

    with patch("france_travail.offres.time.sleep") as mock_sleep:
        result = offres.fetch_all_offres("M1805", "75")
        mock_sleep.assert_called_once_with(1)  # BACKOFF_BASE ** 0 = 1

    assert len(result) == 1


def test_fetch_batch_echoue_apres_max_retries(mock_get, mock_token):
    """
    Vérifie qu'une RuntimeError est levée si le 429 persiste au-delà des MAX_RETRIES.
    Evite une boucle infinie sur une API complètement bloquée.
    """
    response_429 = MagicMock()
    response_429.status_code = 429

    mock_get.return_value = response_429

    with patch("france_travail.offres.time.sleep"):
        with pytest.raises(RuntimeError, match="Échec après"):
            offres.fetch_all_offres("M1805", "75")


# --- Retry 401 ---

def test_fetch_batch_retry_sur_401(mock_get, mock_token, mock_invalidate):
    """
    Vérifie que sur un 401, le token est invalidé et la requête réessayée.
    Le token peut être révoqué côté serveur avant expires_in.
    """
    response_401 = MagicMock()
    response_401.status_code = 401
    response_401.raise_for_status = MagicMock()

    response_ok = _make_response([{"id": "1"}], total=1, start=0, end=0)
    mock_get.side_effect = [response_401, response_ok]

    result = offres.fetch_all_offres("M1805", "75")

    mock_invalidate.assert_called_once()
    assert len(result) == 1


# --- _parse_total ---

def test_parse_total_nominal():
    """Vérifie le parsing du header Content-Range dans le cas standard."""
    assert offres._parse_total("offres 0-149/523") == 523


def test_parse_total_header_absent():
    """Vérifie que _parse_total retourne 0 si le header est absent."""
    assert offres._parse_total("") == 0


def test_parse_total_header_malforme():
    """Vérifie que _parse_total ne plante pas sur un header inattendu."""
    assert offres._parse_total("unexpected_format") == 0


# --- Fixtures ---

@pytest.fixture
def mock_token():
    with patch("france_travail.offres.get_token", return_value="fake_token"):
        yield

@pytest.fixture
def mock_invalidate():
    with patch("france_travail.offres.invalidate_token") as mock:
        yield mock

@pytest.fixture(autouse=True)
def mock_get():
    with patch("france_travail.offres.requests.get") as mock:
        yield mock