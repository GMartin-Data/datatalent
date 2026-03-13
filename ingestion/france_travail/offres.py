import time
import httpx
from auth import get_token, invalidate_token
from config import API_URL, BATCH_SIZE, MAX_OFFRES, MAX_RETRIES, BACKOFF_BASE


def _fetch_batch(code_rome: str, departement: str, start: int, end: int) -> tuple[list, int]:
    for attempt in range(MAX_RETRIES):
        try:
            token = get_token()
            headers = {"Authorization": f"Bearer {token}"}
            params = {
                "codeROME": code_rome,
                "departement": departement,
                "range": f"{start}-{end}"
            }

            response = httpx.get(API_URL, headers=headers, params=params)

            if response.status_code == 429:
                wait = BACKOFF_BASE ** attempt
                print(f"Rate limit (429), attente {wait}s (tentative {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            if response.status_code == 401 and attempt == 0:
                print("Token invalide (401), renouvellement forcé...")
                invalidate_token()
                continue

            response.raise_for_status()
            data = response.json()

            total = _parse_total(response.headers.get("Content-Range", ""))
            offres = data.get("resultats", [])
            return offres, total

        except httpx.HTTPError as e:
            raise

    raise RuntimeError(f"Échec après {MAX_RETRIES} tentatives ({code_rome}, dept {departement})")


def _parse_total(content_range: str) -> int:
    try:
        return int(content_range.split("/")[-1])
    except (IndexError, ValueError):
        return 0


def fetch_all_offres(code_rome: str, departement: str) -> list:
    all_offres = []
    start = 0

    while start < MAX_OFFRES:
        end = min(start + BATCH_SIZE - 1, MAX_OFFRES - 1)
        print(f"Batch {start}-{end} ({code_rome} / dept {departement})")

        offres, total = _fetch_batch(code_rome, departement, start, end)
        all_offres.extend(offres)

        print(f"{len(offres)} offres récupérées (total annoncé : {total})")

        if not offres or start + BATCH_SIZE >= total:
            break

        start += BATCH_SIZE

    print(f"{len(all_offres)} offres au total ({code_rome} / dept {departement})")
    return all_offres