import json
from unittest.mock import patch

from urssaf_masse_salariale.client import fetch_records
from urssaf_masse_salariale.ingest import _transform, run

SECTEUR_NA88I = "62 Programmation, conseil et autres activités informatiques"

MOCK_RESPONSE = {
    "total_count": 2,
    "results": [
        {
            "annee": "2023",
            "secteur_na88i": SECTEUR_NA88I,
            "nombre_d_etablissements": 44000,
            "effectifs_salaries_moyens": 590000,
            "masse_salariale": 32000000000,
        },
        {
            "annee": "2024",
            "secteur_na88i": SECTEUR_NA88I,
            "nombre_d_etablissements": 45000,
            "effectifs_salaries_moyens": 610000,
            "masse_salariale": 33500000000,
        },
    ],
}

RAW_RECORDS = MOCK_RESPONSE["results"]


# --- client ---


@patch("urssaf_masse_salariale.client._fetch_page")
def test_fetch_records_happy_path(mock_fetch_page):
    mock_fetch_page.return_value = MOCK_RESPONSE

    records = fetch_records()

    assert len(records) == 2
    assert records[0]["annee"] == "2023"
    assert records[1]["annee"] == "2024"
    assert "secteur_na88i" in records[0]


# --- ingest ---


def test_transform_happy_path():
    result = _transform(RAW_RECORDS)

    assert len(result) == 2

    row = result[0]
    assert row["code_na88"] == 62
    assert (
        row["libelle_na88"]
        == "Programmation, conseil et autres activités informatiques"
    )
    assert row["annee"] == 2023
    assert row["nb_etablissements"] == 44000
    assert row["effectifs_salaries_moyens"] == 590000
    assert row["masse_salariale_brute"] == 32000000000


@patch("urssaf_masse_salariale.ingest.load_gcs_to_bq")
@patch("urssaf_masse_salariale.ingest.upload_to_gcs")
@patch("urssaf_masse_salariale.ingest.fetch_records")
def test_run_happy_path(mock_fetch, mock_upload, mock_load_bq, tmp_path, monkeypatch):
    mock_fetch.return_value = RAW_RECORDS
    mock_upload.return_value = "gs://datatalent-raw/urssaf_masse_salariale/2026-03-28/urssaf_masse_salariale.jsonl"

    monkeypatch.setattr(
        "urssaf_masse_salariale.ingest.LOCAL_PATH",
        str(tmp_path / "urssaf_masse_salariale.jsonl"),
    )

    run()

    written = (
        (tmp_path / "urssaf_masse_salariale.jsonl").read_text().strip().split("\n")
    )
    first_record = json.loads(written[0])
    assert "_ingestion_date" in first_record

    mock_fetch.assert_called_once()
    mock_upload.assert_called_once()
    mock_load_bq.assert_called_once()
