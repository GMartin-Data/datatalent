import httpx
from shared.logging import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential
from urssaf_effectifs.config import EXPORT_URL, WHERE_FILTER

logger = get_logger(__name__)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_records() -> list[dict]:
    """Récupère tous les enregistrements filtrés sur les 4 codes APE IT.

    Returns:
        Liste de dicts bruts en format wide (champs API tels que retournés
        par Opendatasoft, une ligne par commune x APE).
    """
    logger.info("urssaf_effectifs.fetch_start")
    response = httpx.get(
        EXPORT_URL,
        params={"where": WHERE_FILTER},
        timeout=120,
    )
    response.raise_for_status()
    records = response.json()
    logger.info("urssaf_effectifs.fetch_done", total_records=len(records))
    return records
