from unittest.mock import patch

from urssaf_effectifs.client import fetch_records

MOCK_RECORD = {
    "code_commune": "94065",
    "intitule_commune": "94065 Rungis",
    "code_departement": "94",
    "code_ape": "6203Z",
    "nombre_d_etablissements_2023": 2,
    "effectifs_salaries_2023": 173,
    "nombre_d_etablissements_2024": 2,
    "effectifs_salaries_2024": 176,
}


def _make_page(total: int, records: list) -> dict:
    return {"total_count": total, "results": records}


@patch("urssaf_effectifs.client._fetch_page")
def test_fetch_records_single_page(mock_fetch_page):
    mock_fetch_page.return_value = _make_page(1, [MOCK_RECORD])

    records = fetch_records()

    assert len(records) == 1
    assert records[0]["code_ape"] == "6203Z"
    mock_fetch_page.assert_called_once_with(0)


@patch("urssaf_effectifs.client._fetch_page")
def test_fetch_records_pagination(mock_fetch_page):
    """Vérifie que la boucle pagine correctement sur deux pages."""
    page1 = _make_page(150, [MOCK_RECORD] * 100)
    page2 = _make_page(150, [MOCK_RECORD] * 50)
    mock_fetch_page.side_effect = [page1, page2]

    records = fetch_records()

    assert len(records) == 150
    assert mock_fetch_page.call_count == 2
    mock_fetch_page.assert_any_call(0)
    mock_fetch_page.assert_any_call(100)
