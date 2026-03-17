from datetime import UTC, datetime
from pathlib import Path

import pytest
from sirene.ingest import (
    ResourceInfo,
    build_raw_filename,
    build_resource_info,
    parse_iso_datetime,
    process_one_resource,
    run,
    validate_parquet_magic_number,
    validate_resource_format,
)


def make_resource(
    *,
    logical_name: str = "unite_legale",
    fmt: str = "parquet",
    last_modified: datetime | None = None,
    filename_prefix: str = "StockUniteLegale",
    bq_table: str = "sirene_unite_legale",
) -> ResourceInfo:
    if last_modified is None:
        last_modified = datetime.now(UTC)

    return ResourceInfo(
        logical_name=logical_name,
        resource_id="abc123",
        title="Ressource de test",
        format=fmt,
        mime="application/octet-stream",
        last_modified=last_modified,
        download_url="https://example.com/test.parquet",
        filename_prefix=filename_prefix,
        bq_table=bq_table,
    )


def test_parse_iso_datetime_z_suffix():
    now_utc = datetime.now(UTC).replace(microsecond=0)
    iso_value = now_utc.isoformat().replace("+00:00", "Z")

    dt = parse_iso_datetime(iso_value)

    assert dt.tzinfo is not None
    assert dt == now_utc


def test_build_raw_filename():
    dt = datetime(2026, 3, 5, 10, 30, tzinfo=UTC)
    resource = make_resource(last_modified=dt, filename_prefix="StockUniteLegale")

    assert build_raw_filename(resource) == "StockUniteLegale_2026-03.parquet"


def test_validate_resource_format_ok():
    resource = make_resource(fmt="parquet")
    validate_resource_format(resource, "parquet")


def test_validate_resource_format_raises_for_wrong_format():
    resource = make_resource(fmt="csv")

    with pytest.raises(ValueError, match="Format inattendu"):
        validate_resource_format(resource, "parquet")


def test_validate_parquet_magic_number_ok(tmp_path: Path):
    parquet_path = tmp_path / "test.parquet"
    parquet_path.write_bytes(b"PAR1rest_of_fake_file")

    validate_parquet_magic_number(parquet_path)


def test_validate_parquet_magic_number_raises(tmp_path: Path):
    bad_file = tmp_path / "bad.parquet"
    bad_file.write_bytes(b"XXXXnot_parquet")

    with pytest.raises(ValueError, match="magic number invalide"):
        validate_parquet_magic_number(bad_file)


def test_validate_parquet_magic_number_raises_on_empty_file(tmp_path: Path):
    empty_file = tmp_path / "empty.parquet"
    empty_file.write_bytes(b"")

    with pytest.raises(ValueError, match="magic number invalide"):
        validate_parquet_magic_number(empty_file)


def test_download_file_streams_to_part_then_renames(monkeypatch, tmp_path: Path):
    """Vérifie le flux complet: streaming → .part → rename → validation magic number."""
    resource = make_resource(
        logical_name="unite_legale",
        filename_prefix="StockUniteLegale",
    )
    destination = tmp_path / "StockUniteLegale_2026-03.parquet"
    parquet_content = b"PAR1" + b"\x00" * 1024

    class FakeResponse:
        headers = {"Content-Length": str(len(parquet_content))}

        def raise_for_status(self):
            pass

        def iter_bytes(self, chunk_size: int = 1024):
            yield parquet_content

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def stream(self, method: str, url: str):
            return FakeResponse()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr("sirene.ingest.httpx.Client", FakeClient)

    from sirene.ingest import download_file

    download_file(resource, destination)

    assert destination.exists()
    assert not destination.with_suffix(".parquet.part").exists()
    assert destination.read_bytes() == parquet_content


def test_build_resource_info_raises_if_last_modified_missing():
    dataset_metadata = {
        "resources": [
            {
                "id": "abc-resource",
                "format": "parquet",
                "mime": "application/octet-stream",
                "title": "Test resource",
                "latest": "https://example.com/test.parquet",
            }
        ]
    }

    resource_cfg = {
        "resource_id": "abc-resource",
        "expected_format": "parquet",
        "filename_prefix": "StockUniteLegale",
        "bq_table": "sirene_unite_legale",
    }

    with pytest.raises(ValueError, match="last_modified"):
        build_resource_info("unite_legale", resource_cfg, dataset_metadata)


def test_build_resource_info_raises_if_download_url_missing():
    dataset_metadata = {
        "resources": [
            {
                "id": "abc-resource",
                "format": "parquet",
                "mime": "application/octet-stream",
                "title": "Test resource",
                "last_modified": "2026-03-11T10:00:00Z",
            }
        ]
    }

    resource_cfg = {
        "resource_id": "abc-resource",
        "expected_format": "parquet",
        "filename_prefix": "StockUniteLegale",
        "bq_table": "sirene_unite_legale",
    }

    with pytest.raises(ValueError, match="ni 'latest' ni 'url'"):
        build_resource_info("unite_legale", resource_cfg, dataset_metadata)


def test_process_one_resource_downloads_uploads_and_loads(monkeypatch, tmp_path: Path):
    fake_metadata = {
        "resources": [
            {
                "id": "abc-resource",
                "format": "parquet",
                "mime": "application/octet-stream",
                "title": "Test resource",
                "last_modified": "2026-03-11T10:00:00Z",
                "latest": "https://example.com/test.parquet",
            }
        ]
    }

    resource_cfg = {
        "resource_id": "abc-resource",
        "expected_format": "parquet",
        "filename_prefix": "StockUniteLegale",
        "bq_table": "sirene_unite_legale",
    }

    calls: dict[str, str] = {}

    monkeypatch.setattr("sirene.ingest.RAW_DIR", tmp_path)

    def fake_download_file(resource, destination):
        Path(destination).write_bytes(b"PAR1fakecontent")
        calls["download_destination"] = str(destination)

    def fake_upload_to_gcs(local_path, gcs_prefix):
        calls["upload_local_path"] = local_path
        calls["upload_prefix"] = gcs_prefix
        return "gs://datatalent-raw/sirene/2026-03-13/test.parquet"

    def fake_load_gcs_to_bq(gcs_uri, dataset, table):
        calls["load_gcs_uri"] = gcs_uri
        calls["load_dataset"] = dataset
        calls["load_table"] = table

    monkeypatch.setattr("sirene.ingest.download_file", fake_download_file)
    monkeypatch.setattr("sirene.ingest.upload_to_gcs", fake_upload_to_gcs)
    monkeypatch.setattr("sirene.ingest.load_gcs_to_bq", fake_load_gcs_to_bq)

    gcs_uri = process_one_resource("unite_legale", resource_cfg, fake_metadata)

    assert gcs_uri == "gs://datatalent-raw/sirene/2026-03-13/test.parquet"
    assert calls["upload_prefix"] == "sirene"
    assert calls["load_dataset"] == "raw"
    assert calls["load_table"] == "sirene_unite_legale"
    assert calls["upload_local_path"].endswith("StockUniteLegale_2026-03.parquet")


def test_run_calls_process_for_each_resource(monkeypatch):
    calls = []

    def fake_configure_logging():
        return None

    def fake_ensure_directories():
        return None

    def fake_fetch_dataset_metadata():
        return {"resources": []}

    def fake_process_one_resource(logical_name, resource_cfg, dataset_metadata):
        calls.append((logical_name, resource_cfg, dataset_metadata))
        return f"gs://datatalent-raw/sirene/2026-03-13/{logical_name}.parquet"

    monkeypatch.setattr("sirene.ingest.configure_logging", fake_configure_logging)
    monkeypatch.setattr("sirene.ingest.ensure_directories", fake_ensure_directories)
    monkeypatch.setattr(
        "sirene.ingest.fetch_dataset_metadata", fake_fetch_dataset_metadata
    )
    monkeypatch.setattr("sirene.ingest.process_one_resource", fake_process_one_resource)

    outputs = run()

    assert len(calls) == 2
    assert len(outputs) == 2

    logical_names = {call[0] for call in calls}
    assert logical_names == {"unite_legale", "etablissement"}
