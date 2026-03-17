SCOPE = "api_offresdemploiv2 o2dsoffre"
TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"
RAW_DATA_DIR = "raw/france_travail"
API_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
BATCH_SIZE = 150
MAX_OFFRES = 1_150
MAX_RETRIES = 5
BACKOFF_BASE = 2
codes_rome = ["M1805"]
departements = ["75"]
