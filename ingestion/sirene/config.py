from pathlib import Path

# Dataset officiel Base Sirene sur data.gouv
DATA_GOUV_DATASET_ID = "5b7ffc618b4c4169d30727e0"
DATA_GOUV_API_DATASET_URL = (
    f"https://www.data.gouv.fr/api/1/datasets/{DATA_GOUV_DATASET_ID}/"
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PREPARED_DIR = DATA_DIR / "prepared"

HTTP_TIMEOUT_SECONDS = 60
CHUNK_SIZE = 1024 * 1024  # 1 Mo
MAX_RESOURCE_AGE_DAYS = 45

SIRENE_RESOURCES = {
    "unite_legale": {
        "resource_id": "350182c9-148a-46e0-8389-76c2ec1374a3",
        "expected_format": "parquet",
        "filename_prefix": "StockUniteLegale",
    },
    "etablissement": {
        "resource_id": "a29c1297-1f92-4e2a-8f6b-8c902ce96c5f",
        "expected_format": "parquet",
        "filename_prefix": "StockEtablissement",
    },
}

# Colonnes conservées après exploration
# Établissement : on garde les colonnes utiles à la jointure, la géographie,
# le statut, l'activité et quelques enrichissements utiles.
ETABLISSEMENT_COLUMNS = [
    "siren",
    "nic",
    "siret",
    "dateCreationEtablissement",
    "trancheEffectifsEtablissement",
    "anneeEffectifsEtablissement",
    "etablissementSiege",
    "numeroVoieEtablissement",
    "typeVoieEtablissement",
    "libelleVoieEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    "codeCommuneEtablissement",
    "dateDebut",
    "etatAdministratifEtablissement",
    "enseigne1Etablissement",
    "denominationUsuelleEtablissement",
    "activitePrincipaleEtablissement",
    "nomenclatureActivitePrincipaleEtablissement",
    "caractereEmployeurEtablissement",
    "activitePrincipaleNAF25Etablissement",
    "statutDiffusionEtablissement",
    "dateDernierTraitementEtablissement",
    "nombrePeriodesEtablissement",
    "identifiantAdresseEtablissement",
    "coordonneeLambertAbscisseEtablissement",
    "coordonneeLambertOrdonneeEtablissement",
]

# Unité légale : on garde les colonnes utiles au nom d'entreprise,
# au regroupement par SIREN, au statut, à l'activité et à la structure.
UNITE_LEGALE_COLUMNS = [
    "siren",
    "dateCreationUniteLegale",
    "trancheEffectifsUniteLegale",
    "anneeEffectifsUniteLegale",
    "categorieEntreprise",
    "anneeCategorieEntreprise",
    "dateDebut",
    "etatAdministratifUniteLegale",
    "nomUniteLegale",
    "nomUsageUniteLegale",
    "denominationUniteLegale",
    "denominationUsuelle1UniteLegale",
    "sigleUniteLegale",
    "categorieJuridiqueUniteLegale",
    "activitePrincipaleUniteLegale",
    "nomenclatureActivitePrincipaleUniteLegale",
    "nicSiegeUniteLegale",
    "activitePrincipaleNAF25UniteLegale",
    "statutDiffusionUniteLegale",
    "dateDernierTraitementUniteLegale",
    "nombrePeriodesUniteLegale",
    "economieSocialeSolidaireUniteLegale",
]

RESOURCE_SELECTED_COLUMNS = {
    "etablissement": ETABLISSEMENT_COLUMNS,
    "unite_legale": UNITE_LEGALE_COLUMNS,
}
