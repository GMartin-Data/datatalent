"""Constantes de configuration pour l'ingestion France Travail."""

# --- OAuth2 ---
TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"
SCOPE = "api_offresdemploiv2 o2dsoffre"

# --- API Offres d'emploi v2 ---
API_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
BATCH_SIZE = 150
MAX_OFFRES = 1_150
SLEEP_BETWEEN_REQUESTS = 0.15  # Rate limit 10/s

# --- Codes ROME (D8) ---
CODES_ROME = ["M1805", "M1810", "M1806", "M1801"]

# --- Départements (101 : 01-95, 2A, 2B, 971-976) ---
DEPARTEMENTS = [
    *(f"{i:02d}" for i in range(1, 20)),
    "2A",
    "2B",
    *(f"{i:02d}" for i in range(21, 96)),
    *(
        str(i) for i in range(971, 975)
    ),  # Exclut Saint-Pierre et Miquelon (collectivité, pas département)
    "976",
]

# --- Sortie locale ---
OUTPUT_DIR = "/tmp"
