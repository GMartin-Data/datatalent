from unittest.mock import patch

from urssaf_masse_salariale.client import fetch_records

MOCK_RESPONSE = {
    "total_count": 2,
    "results": [
        {
            "annee": 2023,
            "na88": "62 Programmation, conseil et autres activités informatiques",
            "nombre_d_etablissements": 44000,
            "effectifs_salaries_moyens": 590000,
            "masse_salariale_brute": 32000000000,
        },
        {
            "annee": 2024,
            "na88": "62 Programmation, conseil et autres activités informatiques",
            "nombre_d_etablissements": 45000,
            "effectifs_salaries_moyens": 610000,
            "masse_salariale_brute": 33500000000,
        },
    ],
}


@patch("urssaf_masse_salariale.client._fetch_page")
def test_fetch_records_happy_path(mock_fetch_page):
    mock_fetch_page.return_value = MOCK_RESPONSE

    records = fetch_records()

    assert len(records) == 2
    assert records[0]["annee"] == 2023
    assert records[1]["annee"] == 2024
    assert "na88" in records[0]
