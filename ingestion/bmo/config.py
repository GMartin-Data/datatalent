"""Configuration pour l'ingestion BMO France Travail."""

# URL du fichier XLSX BMO 2025 (téléchargement libre, aucune auth)
BMO_XLSX_URL = (
    "https://www.francetravail.org/files/live/sites/peorg/files/"
    "documents/Statistiques-et-analyses/Open-data/BMO/"
    "Base_open_data_BMO_2025.xlsx"
)

# Onglet contenant les données (50 076 lignes, en-têtes ligne 1)
BMO_SHEET_NAME = "BMO_2025_open_data"

# 6 codes métier IT (nomenclature FAP2021, préfixes M1X/M2X)
# M1X = techniciens informatiques, M2X = cadres informatiques
# Ces codes sont dispersés dans les familles A et C — le filtre
# s'applique sur "Code métier BMO", pas sur "Famille_met".
CODES_METIER_IT: frozenset[str] = frozenset(
    {
        "M1X80",  # Techniciens d'étude et de développement en informatique
        "M1X81",  # Techniciens de production, exploitation, installation, support
        "M2X90",  # Ingénieurs et cadres d'étude, R&D en informatique et télécom
        "M2X91",  # Ingénieurs et cadres d'administration, maintenance en informatique
        "M2X92",  # Ingénieurs et cadres des télécommunications
        "M2X93",  # Experts et consultants en systèmes d'information
    }
)

# Mapping colonnes source (en-têtes XLSX) → colonnes JSONL
# L'ordre du dict détermine l'ordre des champs dans le JSONL.
# Les colonnes numériques (met, xmet, smet) subissent un cast int
# avec gestion du secret statistique ("*" → None) — logique dans parse_xlsx.
COLUMN_MAPPING: dict[str, str] = {
    "annee": "annee",
    "Code métier BMO": "code_metier_bmo",
    "Nom métier BMO": "libelle_metier_bmo",
    "Famille_met": "code_famille_metier",
    "Lbl_fam_met": "libelle_famille_metier",
    "REG": "code_region",
    "NOM_REG": "nom_region",
    "Dept": "code_departement",
    "NomDept": "nom_departement",
    "BE25": "code_bassin_emploi",
    "NOMBE25": "libelle_bassin_emploi",
    "met": "projets_recrutement",
    "xmet": "projets_difficiles",
    "smet": "projets_saisonniers",
}

# Colonnes source qui contiennent des valeurs numériques avec secret statistique
# "*" → None, sinon cast int
NUMERIC_COLUMNS: frozenset[str] = frozenset({"met", "xmet", "smet"})

# Préfixe GCS pour le upload (convention shared/)
GCS_PREFIX = "bmo"

# Table BigQuery cible
BQ_DATASET = "raw"
BQ_TABLE = "bmo"
