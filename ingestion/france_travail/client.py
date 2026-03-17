"""Client API France Travail — OAuth2 + fetch offres."""

import time

import httpx

from .config import (
    API_URL,
    BACKOFF_BASE,
    BATCH_SIZE,
    MAX_OFFRES,
    MAX_RETRIES,
    SCOPE,
    TOKEN_URL,
)


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

        print(
            f"Nouveau token obtenu, expire dans {token_info.get('expires_in', 1500)}s"
        )
        return self._token_cache["access_token"]

    def _invalidate_token(self):
        """Force le renouvellement du token au prochain appel."""
        self._token_cache["expires_at"] = 0

    # --- Fetch (public) ---

    def _fetch_batch(
        self, code_rome: str, departement: str, start: int, end: int
    ) -> tuple[list, int]:
        """Récupère un batch d'offres (une page de résultats)."""
        for attempt in range(MAX_RETRIES):
            try:
                token = self._get_token()
                headers = {"Authorization": f"Bearer {token}"}
                params = {
                    "codeROME": code_rome,
                    "departement": departement,
                    "range": f"{start}-{end}",
                }

                response = self._http.get(API_URL, headers=headers, params=params)

                if response.status_code == 429:
                    wait = BACKOFF_BASE**attempt
                    print(
                        f"Rate limit (429), attente {wait}s "
                        f"(tentative {attempt + 1}/{MAX_RETRIES})"
                    )
                    time.sleep(wait)
                    continue

                if response.status_code == 401 and attempt == 0:
                    print("Token invalide (401), renouvellement forcé...")
                    self._invalidate_token()
                    continue

                response.raise_for_status()
                data = response.json()

                total = self._parse_total(response.headers.get("Content-Range", ""))
                offres = data.get("resultats", [])
                return offres, total

            except httpx.HTTPError:
                raise

        raise RuntimeError(
            f"Échec après {MAX_RETRIES} tentatives ({code_rome}, dept {departement})"
        )

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
            print(f"Batch {start}-{end} ({code_rome} / dept {departement})")

            offres, total = self._fetch_batch(code_rome, departement, start, end)
            all_offres.extend(offres)

            print(f"{len(offres)} offres récupérées (total annoncé : {total})")

            if not offres or start + BATCH_SIZE >= total:
                break

            start += BATCH_SIZE

        print(f"{len(all_offres)} offres au total ({code_rome} / dept {departement})")
        return all_offres

    # --- Cleanup ---

    def close(self):
        """Ferme le client HTTP."""
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
