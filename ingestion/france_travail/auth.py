import os
import time
from urllib import response
import httpx 
from config import TOKEN_URL, SCOPE
# tenacity

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

_token_cache = {
    "access_token": None,
    "expires_at": 0
}

def get_token() -> str:
    now = time.time()
    margin = 60

    if _token_cache["access_token"] and now < _token_cache["expires_at"] - margin:
        return _token_cache["access_token"]

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    }

    response = httpx.post(TOKEN_URL, data=data, headers=headers)
    response.raise_for_status()
    token_info = response.json()

    _token_cache["access_token"] = token_info["access_token"]
    _token_cache["expires_at"] = now + token_info.get("expires_in", 1500)

    print(f"Nouveau token obtenu, expire dans {token_info.get('expires_in', 1500)}s")
    return _token_cache["access_token"]


def invalidate_token():
    """Force le renouvellement du token au prochain appel."""
    _token_cache["expires_at"] = 0