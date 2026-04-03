"""Client API France Travail — OAuth2 + fetch offres."""

import time

import httpx
import tenacity
from shared.logging import get_logger

from .config import (
    API_URL,
    BATCH_SIZE,
    MAX_OFFRES,
    SCOPE,
    SLEEP_BETWEEN_REQUESTS,
    TOKEN_URL,
)

logger = get_logger(__name__)


class RetryableAPIError(Exception):
    """Erreur API retryable (429, 500, 503)."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


class FranceTravailClient:
    """Client pour l'API Offres d'emploi v2 de France Travail.

    Gère l'authentification OAuth2 (client_credentials) avec cache du token,
    et la récupération paginée des offres par code ROME et département.
    """

    def __init__(self, client_id: str, client_secret: str):
        self._client_id = client_id
        self._client_secret = client_secret
        self._http = httpx.Client(timeout=30)
        self._token_cache = {
            "access_token": None,
            "expires_at": 0,
        }

    # --- Auth (privé) ---

    def _get_token(self) -> str:
        """Retourne un token valide, avec cache et renouvellement anticipé ."""
        now = time.time()
        margin = 60

        if (
            self._token_cache["access_token"]
            and now < self._token_cache["expires_at"] - margin
        ):
            return self._token_cache["access_token"]

        response = self._http.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": SCOPE,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        token_info = response.json()

        self._token_cache["access_token"] = token_info["access_token"]
        self._token_cache["expires_at"] = now + token_info.get("expires_in", 1500)

        logger.info("token_obtained", expires_in=token_info.get("expires_in", 1500))
        return self._token_cache["access_token"]

    def _invalidate_token(self):
        """Force le renouvellement du token au prochain appel."""
        self._token_cache["expires_at"] = 0

    # --- Fetch (public) ---

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(RetryableAPIError),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=30)
        + tenacity.wait_random(0, 1),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    def _request(
        self, code_rome: str, departement: str, start: int, end: int
    ) -> httpx.Response:
        """Exécute un appel HTTP unique. Lève RetryableAPIError sur 429/500/503."""
        token = self._get_token()
        response = self._http.get(
            API_URL,
            headers={"Authorization": f"Bearer {token}"},
            params={
                "codeROME": code_rome,
                "departement": departement,
                "range": f"{start}-{end}",
            },
        )

        if response.status_code in (429, 500, 503):
            raise RetryableAPIError(
                response.status_code,
                f"HTTP {response.status_code} pour {code_rome}/dept {departement}",
            )

        return response

    def _fetch_batch(
        self, code_rome: str, departement: str, start: int, end: int
    ) -> tuple[list, int]:
        """Récupère un batch d'offres (une page de résultats)."""
        response = self._request(code_rome, departement, start, end)

        if response.status_code == 401:
            self._invalidate_token()
            response = self._request(code_rome, departement, start, end)

        if response.status_code == 204:
            return [], 0

        response.raise_for_status()  # Crashe proprement sur les autres erreurs
        data = response.json()

        total = self._parse_total(response.headers.get("Content-Range", ""))
        offres = data.get("resultats", [])
        return offres, total

    @staticmethod
    def _parse_total(content_range: str) -> int:
        """Extrait le total depuis le header Content-Range."""
        try:
            return int(content_range.split("/")[-1])
        except (IndexError, ValueError):
            return 0

    def fetch_offres(self, code_rome: str, departement: str) -> list[dict]:
        """Récupère toutes les offres pour un code ROME × département."""
        all_offres = []
        start = 0

        while start < MAX_OFFRES:
            end = min(start + BATCH_SIZE - 1, MAX_OFFRES - 1)
            logger.debug(
                "fetch_batch",
                range=f"{start}-{end}",
                code_rome=code_rome,
                departement=departement,
            )

            time.sleep(SLEEP_BETWEEN_REQUESTS)
            offres, total = self._fetch_batch(code_rome, departement, start, end)
            all_offres.extend(offres)

            logger.debug("batch_result", count=len(offres), total=total)

            if not offres or start + BATCH_SIZE >= total:
                break

            start += BATCH_SIZE

        logger.info(
            "fetch_complete",
            count=len(all_offres),
            code_rome=code_rome,
            departement=departement,
        )
        return all_offres

    # --- Cleanup ---

    def close(self):
        """Ferme le client HTTP."""
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
