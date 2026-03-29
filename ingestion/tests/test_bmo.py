"""Tests pour ingestion/bmo/parse_xlsx.py."""

import pytest
from bmo.config import BMO_SHEET_NAME, COLUMN_MAPPING
from bmo.parse_xlsx import parse_bmo_xlsx
from openpyxl import Workbook

# En-têtes source dans l'ordre du COLUMN_MAPPING
SOURCE_HEADERS = list(COLUMN_MAPPING.keys())


def _create_bmo_xlsx(path, rows: list[list]):
    """Crée un XLSX minimal avec l'onglet BMO et les lignes fournies."""
    wb = Workbook()
    ws = wb.active
    ws.title = BMO_SHEET_NAME

    # Ligne 1 : en-têtes
    ws.append(SOURCE_HEADERS)

    # Lignes de données
    for row in rows:
        ws.append(row)

    wb.save(path)


# Lignes de test — ordre = celui de SOURCE_HEADERS
# annee, Code métier BMO, Nom métier BMO, Famille_met, Lbl_fam_met,
# REG, NOM_REG, Dept, NomDept, BE25, NOMBE25, met, xmet, smet
ROW_IT_NORMAL = [
    "2025",
    "M1X80",
    "Techniciens développement",
    "A",
    "Fonctions administratives",
    "11",
    "Ile-de-France",
    "75",
    "Paris",
    "5001",
    "T1 Paris",
    "2824",
    "1158",
    "11",
]

ROW_IT_SECRET = [
    "2025",
    "M2X90",
    "Ingénieurs R&D info",
    "C",
    "Fonctions d'encadrement",
    "84",
    "Auvergne-Rhone-Alpes",
    "69",
    "Rhône",
    "8201",
    "LYON",
    "526",
    "371",
    "*",
]

ROW_NON_IT = [
    "2025",
    "V4Z82",
    "Serveurs cafés restaurants",
    "V",
    "Vente tourisme services",
    "11",
    "Ile-de-France",
    "75",
    "Paris",
    "5001",
    "T1 Paris",
    "450",
    "200",
    "100",
]

ROW_IT_AUTRE_CODE = [
    "2025",
    "M2X93",
    "Experts SI",
    "C",
    "Fonctions d'encadrement",
    "59",
    "Nord",
    "59",
    "Nord",
    "3101",
    "LILLE",
    "180",
    "90",
    "20",
]


@pytest.fixture()
def bmo_xlsx(tmp_path):
    """Fixture : XLSX BMO avec 4 lignes (2 IT normales, 1 IT secret, 1 non-IT)."""
    path = tmp_path / "bmo_test.xlsx"
    _create_bmo_xlsx(
        path, [ROW_IT_NORMAL, ROW_IT_SECRET, ROW_NON_IT, ROW_IT_AUTRE_CODE]
    )
    return path


class TestParseBmoXlsx:
    """Tests du happy path de parse_bmo_xlsx."""

    def test_filtre_lignes_non_it(self, bmo_xlsx):
        """Seules les lignes avec un code métier IT sont retenues."""
        records = parse_bmo_xlsx(bmo_xlsx)
        assert len(records) == 3  # 2 IT normales + 1 IT secret, pas la non-IT

        codes = {r["code_metier_bmo"] for r in records}
        assert codes == {"M1X80", "M2X90", "M2X93"}

    def test_mapping_colonnes(self, bmo_xlsx):
        """Les clés JSONL correspondent au COLUMN_MAPPING."""
        records = parse_bmo_xlsx(bmo_xlsx)
        expected_keys = set(COLUMN_MAPPING.values())
        for record in records:
            assert set(record.keys()) == expected_keys

    def test_cast_annee_int(self, bmo_xlsx):
        """annee est castée en int, pas gardée en string."""
        records = parse_bmo_xlsx(bmo_xlsx)
        for record in records:
            assert record["annee"] == 2025
            assert isinstance(record["annee"], int)

    def test_valeurs_numeriques_normales(self, bmo_xlsx):
        """met/xmet/smet sont castées en int quand non masquées."""
        records = parse_bmo_xlsx(bmo_xlsx)
        paris = next(r for r in records if r["code_departement"] == "75")
        assert paris["projets_recrutement"] == 2824
        assert paris["projets_difficiles"] == 1158
        assert paris["projets_saisonniers"] == 11

    def test_secret_statistique_none(self, bmo_xlsx):
        """Les valeurs "*" sont converties en None, pas en 0."""
        records = parse_bmo_xlsx(bmo_xlsx)
        lyon = next(r for r in records if r["code_departement"] == "69")
        assert lyon["projets_recrutement"] == 526
        assert lyon["projets_difficiles"] == 371
        assert lyon["projets_saisonniers"] is None

    def test_colonnes_texte_str(self, bmo_xlsx):
        """Les colonnes texte restent en str (pas de cast inattendu)."""
        records = parse_bmo_xlsx(bmo_xlsx)
        paris = next(r for r in records if r["code_departement"] == "75")
        assert paris["nom_region"] == "Ile-de-France"
        assert paris["code_region"] == "11"
        assert isinstance(paris["code_bassin_emploi"], str)


class TestParseBmoXlsxErrors:
    """Tests des cas d'erreur."""

    def test_colonne_manquante_raise_valueerror(self, tmp_path):
        """Un XLSX avec une colonne manquante lève ValueError."""
        path = tmp_path / "bmo_bad.xlsx"
        wb = Workbook()
        ws = wb.active
        ws.title = BMO_SHEET_NAME
        # En-têtes tronquées — on enlève les 3 dernières colonnes
        ws.append(SOURCE_HEADERS[:-3])
        wb.save(path)

        with pytest.raises(ValueError, match="Colonnes attendues absentes"):
            parse_bmo_xlsx(path)
