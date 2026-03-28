import re

BASE_URL = (
    "https://open.urssaf.fr/api/explore/v2.1/catalog/datasets"
    "/etablissements-et-effectifs-salaries-au-niveau-commune-x-ape-last"
    "/records"
)

WHERE_FILTER = 'code_ape IN ("6201Z", "6202A", "6203Z", "6209Z")'
ORDER_BY = "commune ASC"
PAGE_LIMIT = 100

# Codes APE IT retenus (D37)
APE_IT = {"6201Z", "6202A", "6203Z", "6209Z"}

# Champs dimensionnels à conserver
DIMENSION_FIELDS = [
    "code_commune",
    "intitule_commune",
    "code_departement",
    "code_ape",
]

# Pattern de détection des colonnes wide
PATTERN_EFFECTIFS = re.compile(r"^effectifs_salaries_(\d{4})$")
PATTERN_ETABLISSEMENTS = re.compile(r"^nombre_d_etablissements_(\d{4})$")

GCS_PREFIX = "urssaf_effectifs"
BQ_DATASET = "raw"
BQ_TABLE = "urssaf_effectifs"
LOCAL_PATH = "/tmp/urssaf_effectifs.jsonl"
DUMP_PATH = "/tmp/urssaf_effectifs_dump.jsonl"
