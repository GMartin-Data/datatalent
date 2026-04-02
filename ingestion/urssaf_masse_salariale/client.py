import httpx
from shared.logging import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential
from urssaf_masse_salariale.config import (
    BASE_URL,
    ORDER_BY,
    PAGE_LIMIT,
    WHERE_FILTER,
)

logger = get_logger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def _fetch_page(offset: int) -> dict:
    params = {
        "where": WHERE_FILTER,
        "order_by": ORDER_BY,
        "limit": PAGE_LIMIT,
        "offset": offset,
    }
    response = httpx.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_records() -> list[dict]:
    """Récupère tous les enregistrements NA88=62 depuis l'API URSSAF.

    Returns:
        Liste de dicts bruts (champs API tels que retournés par OpenDataSoft).
    """
    all_records = []
    offset = 0

    while True:
        logger.info("urssaf_masse_salariale.fetch_page", offset=offset)
        data = _fetch_page(offset)
        records = data.get("results", [])
        all_records.extend(records)

        total = data.get("total_count", 0)
        offset += PAGE_LIMIT

        if offset >= total:
            break

    logger.info("urssaf_masse_salariale.fetch_done", total_records=len(all_records))
    return all_records
