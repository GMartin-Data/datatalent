"""Configuration pour l'ingestion Adzuna."""

import os

# --- Endpoint API ---
BASE_URL = "https://api.adzuna.com/v1/api/jobs/fr/search"


def get_credentials() -> tuple[str, str]:
    """Retourne (app_id, app_key) depuis les variables d'environnement.

    Raises:
        ValueError: si une variable est manquante.
    """
    app_id = os.environ.get("ADZUNA_APP_ID")
    app_key = os.environ.get("ADZUNA_APP_KEY")
    if not app_id or not app_key:
        missing = [
            v for v in ("ADZUNA_APP_ID", "ADZUNA_APP_KEY") if not os.environ.get(v)
        ]
        raise ValueError(f"Variables d'environnement manquantes : {', '.join(missing)}")
    return app_id, app_key


# --- Paramètres de requête ---
SEARCH_QUERY = "data+engineer"
RESULTS_PER_PAGE = 50
REQUEST_TIMEOUT = 30  # secondes — pages profondes lentes (spike)
SLEEP_BETWEEN_REQUESTS = 2.5  # 25 req/min → 1 req/2.4s, marge à 2.5s

# --- Fichier local ---
LOCAL_JSONL_PATH = "/tmp/adzuna.jsonl"

# --- GCS / BigQuery ---
GCS_PREFIX = "adzuna"
BQ_DATASET = "raw"
BQ_TABLE = "adzuna"

# --- Mapping colonnes source → JSONL ---
# Clé = chemin dans le JSON source (notation pointée pour champs imbriqués)
# Valeur = nom de colonne dans le JSONL
COLUMN_MAP: dict[str, str] = {
    "id": "offre_id",
    "title": "titre",
    "company.display_name": "entreprise_nom",
    "location.display_name": "localisation_libelle",
    "location.area": "localisation_area",
    "latitude": "latitude",
    "longitude": "longitude",
    "salary_min": "salaire_min",
    "salary_max": "salaire_max",
    "salary_is_predicted": "salaire_est_estime",
    "description": "description",
    "redirect_url": "redirect_url",
    "category.tag": "categorie_tag",
    "category.label": "categorie_libelle",
    "contract_type": "type_contrat",
    "contract_time": "temps_travail",
    "created": "date_creation",
}
