import re

EXPORT_URL = (
    "https://open.urssaf.fr/api/explore/v2.1/catalog/datasets"
    "/etablissements-et-effectifs-salaries-au-niveau-commune-x-ape-last"
    "/exports/json"
)

# Codes APE IT retenus (D37)
WHERE_FILTER = 'code_ape IN ("6201Z", "6202A", "6203Z", "6209Z")'

# Champs dimensionnels à conserver
DIMENSION_FIELDS = [
    "code_commune",
    "intitule_commune",
    "code_departement",
    "code_ape",
]

# Pattern de détection des colonnes wide
PATTERN_EFFECTIFS = re.compile(r"^effectifs_salaries_(\d{4})$")

GCS_PREFIX = "urssaf_effectifs"
BQ_DATASET = "raw"
BQ_TABLE = "urssaf_effectifs"
LOCAL_PATH = "/tmp/urssaf_effectifs.jsonl"
