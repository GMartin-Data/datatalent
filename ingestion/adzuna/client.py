"""Client API Adzuna - requêtes paginées avec retry et rate limiting."""

import time

import httpx
import tenacity
from shared.logging import get_logger

from .config import (
    BASE_URL,
    REQUEST_TIMEOUT,
    RESULTS_PER_PAGE,
    SEARCH_QUERY,
    SLEEP_BETWEEN_REQUESTS,
)

logger = get_logger(__name__)


class RetryableAPIError(Exception):
    """Erreur API retryable (429, 500, 503)."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(RetryableAPIError),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=16)
    + tenacity.wait_random(0, 1),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def _fetch_page(
    client: httpx.Client,
    page: int,
    app_id: str,
    app_key: str,
) -> dict:
    """Requête GET pour une page de résultats Adzuna.

    Args:
        client: instance httpx.Client (connexion réutilisée).
        page: numéro de page (1-indexed).
        app_id: identifiant application Adzuna.
        app_key: clé API Adzuna.

    Returns:
        Réponse JSON parsée (dict avec clés 'results', 'count', etc.).

    Raises:
        RetryableAPIError: sur HTTP 429/500/503. Retryé automatiquement
            par tenacity (backoff exponentiel 2-16s + jitter, max 5 tentatives).
        httpx.HTTPStatusError: sur tout autre code HTTP non-2xx.
    """
    url = f"{BASE_URL}/{page}"
    response = client.get(
        url,
        params={
            "app_id": app_id,
            "app_key": app_key,
            "what": SEARCH_QUERY,
            "results_per_page": RESULTS_PER_PAGE,
            "content-type": "application/json",
        },
    )

    if response.status_code in (429, 500, 503):
        raise RetryableAPIError(
            response.status_code,
            f"HTTP {response.status_code} page {page}",
        )

    response.raise_for_status()
    return response.json()


def fetch_all_offers(app_id: str, app_key: str) -> list[dict]:
    """Récupère toutes les offres via pagination.

    Args:
        app_id: identifiant application Adzuna.
        app_key: clé API Adzuna.

    Returns:
        Liste des offres brutes (dicts non transformés).
    """
    all_offers: list[dict] = []
    page = 1

    with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
        while True:
            logger.info("fetch_page", page=page)
            data = _fetch_page(client, page, app_id, app_key)

            results = data.get("results", [])
            count = data.get("count", 0)
            all_offers.extend(results)

            logger.info(
                "page_fetched",
                page=page,
                results_in_page=len(results),
                total_so_far=len(all_offers),
                count=count,
            )

            if len(results) < RESULTS_PER_PAGE or page * RESULTS_PER_PAGE >= count:
                break

            page += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    logger.info("fetch_complete", total_offers=len(all_offers))
    return all_offers
