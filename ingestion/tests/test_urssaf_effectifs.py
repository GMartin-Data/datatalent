from unittest.mock import MagicMock, patch

from urssaf_effectifs.client import fetch_records
from urssaf_effectifs.ingest import _unpivot, run

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

WIDE_RECORD = {
    "code_commune": "94065",
    "intitule_commune": "94065 Rungis",
    "code_departement": "94",
    "code_ape": "6203Z",
    "effectifs_salaries_2022": 178,
    "nombre_d_etablissements_2022": 2,
    "effectifs_salaries_2023": 173,
    "nombre_d_etablissements_2023": 2,
    "effectifs_salaries_2024": None,
    "nombre_d_etablissements_2024": None,
}


# --- client ---


@patch("urssaf_effectifs.client.httpx.get")
def test_fetch_records_happy_path(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = [MOCK_RECORD]
    mock_get.return_value = mock_response

    records = fetch_records()

    assert len(records) == 1
    assert records[0]["code_ape"] == "6203Z"
    mock_get.assert_called_once()


# --- ingest ---


def test_unpivot_happy_path():
    result = _unpivot([WIDE_RECORD])

    assert len(result) == 2

    annees = {r["annee"] for r in result}
    assert annees == {2022, 2023}

    row_2023 = next(r for r in result if r["annee"] == 2023)
    assert row_2023["code_commune"] == "94065"
    assert row_2023["code_ape"] == "6203Z"
    assert row_2023["effectifs_salaries"] == 173
    assert row_2023["nb_etablissements"] == 2


def test_unpivot_filters_null_years():
    record = {
        **WIDE_RECORD,
        "effectifs_salaries_2022": None,
        "nombre_d_etablissements_2022": None,
    }
    result = _unpivot([record])

    annees = {r["annee"] for r in result}
    assert 2022 not in annees
    assert 2023 in annees


@patch("urssaf_effectifs.ingest.load_gcs_to_bq")
@patch("urssaf_effectifs.ingest.upload_to_gcs")
@patch("urssaf_effectifs.ingest.fetch_records")
@patch("urssaf_effectifs.ingest._write_jsonl")
def test_run_happy_path(mock_write, mock_fetch, mock_upload, mock_load_bq):
    mock_fetch.return_value = [WIDE_RECORD]
    mock_upload.return_value = (
        "gs://datatalent-raw/urssaf_effectifs/2026-03-28/urssaf_effectifs.jsonl"
    )

    run()

    mock_fetch.assert_called_once()
    mock_write.assert_called_once()
    mock_upload.assert_called_once()
    mock_load_bq.assert_called_once()
