from unittest.mock import patch

from geo.ingest import fetch_geo_data


@patch("geo.ingest.requests.get")
def test_fetch_geo_data(mock_get):
    fausses_donnees = [{"code": "11", "nom": "Île-de-France"}]
    mock_get.return_value.json.return_value = fausses_donnees

    resultat = fetch_geo_data("regions")

    assert resultat == fausses_donnees
