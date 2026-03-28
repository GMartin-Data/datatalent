BASE_URL = (
    "https://open.urssaf.fr/api/explore/v2.1/catalog/datasets"
    "/nombre-etab-effectifs-salaries-et-masse-salariale-secteur-prive-france-x-na88"
    "/records"
)

WHERE_FILTER = "na88 LIKE '62%'"
ORDER_BY = "annee ASC"
PAGE_LIMIT = 100

# Mapping champs API → colonnes JSONL de sortie
# na88 est traité séparément (éclatement code + libellé)
FIELD_MAP = {
    "annee": "annee",
    "nombre_d_etablissements": "nb_etablissements",
    "effectifs_salaries_moyens": "effectifs_salaries_moyens",
    "masse_salariale_brute": "masse_salariale_brute",
}

GCS_PREFIX = "urssaf_masse_salariale"
BQ_DATASET = "raw"
BQ_TABLE = "urssaf_masse_salariale"
LOCAL_PATH = "/tmp/urssaf_masse_salariale.jsonl"
