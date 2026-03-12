from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sirene.ingest import (
    ResourceInfo,
    build_prepared_filename,
    build_raw_filename,
    parse_iso_datetime,
    run,
    select_existing_columns,
    transform_parquet_keep_columns,
    validate_resource_format,
    validate_resource_freshness,
)


def make_resource(
    *,
    logical_name: str = "unite_legale",
    fmt: str = "parquet",
    last_modified: datetime | None = None,
    filename_prefix: str = "StockUniteLegale",
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


def test_build_prepared_filename():
    dt = datetime(2026, 3, 5, 10, 30, tzinfo=UTC)
    resource = make_resource(last_modified=dt, filename_prefix="StockEtablissement")

    assert (
        build_prepared_filename(resource) == "StockEtablissement_2026-03_light.parquet"
    )


def test_select_existing_columns_preserves_required_order():
    available = ["col_a", "col_b", "col_c", "col_d"]
    required = ["col_c", "col_x", "col_a"]

    result = select_existing_columns(available, required)

    assert result == ["col_c", "col_a"]


def test_validate_resource_format_ok():
    resource = make_resource(fmt="parquet")
    validate_resource_format(resource, "parquet")


def test_validate_resource_format_raises_for_wrong_format():
    resource = make_resource(fmt="csv")

    with pytest.raises(ValueError, match="Format inattendu"):
        validate_resource_format(resource, "parquet")


def test_validate_resource_freshness_ok():
    recent_dt = datetime.now(UTC) - timedelta(days=5)
    resource = make_resource(last_modified=recent_dt)

    validate_resource_freshness(resource, max_age_days=45)


def test_validate_resource_freshness_raises_for_old_resource():
    old_dt = datetime.now(UTC) - timedelta(days=90)
    resource = make_resource(last_modified=old_dt)

    with pytest.raises(ValueError, match="Ressource trop ancienne"):
        validate_resource_freshness(resource, max_age_days=45)


def test_transform_parquet_keep_columns_writes_only_selected_columns(tmp_path: Path):
    source_path = tmp_path / "source.parquet"
    destination_path = tmp_path / "prepared.parquet"

    table = pa.table(
        {
            "siren": ["111", "222"],
            "nom": ["A", "B"],
            "categorie": ["X", "Y"],
            "extra": [1, 2],
        }
    )
    pq.write_table(table, source_path)

    kept_columns = transform_parquet_keep_columns(
        source_path=source_path,
        destination_path=destination_path,
        selected_columns=["nom", "siren"],
    )

    assert kept_columns == ["nom", "siren"]
    assert destination_path.exists()

    result = pq.read_table(destination_path)
    assert result.column_names == ["nom", "siren"]
    assert result.num_rows == 2


def test_transform_parquet_keep_columns_raises_if_no_requested_column_exists(
    tmp_path: Path,
):
    source_path = tmp_path / "source.parquet"
    destination_path = tmp_path / "prepared.parquet"

    table = pa.table(
        {
            "col1": [1, 2],
            "col2": [3, 4],
        }
    )
    pq.write_table(table, source_path)

    with pytest.raises(ValueError, match="Aucune colonne demandée"):
        transform_parquet_keep_columns(
            source_path=source_path,
            destination_path=destination_path,
            selected_columns=["inexistante_a", "inexistante_b"],
        )


def test_run_calls_process_for_each_resource(monkeypatch):
    calls = []

    def fake_ensure_directories():
        return None

    def fake_fetch_dataset_metadata():
        return {"resources": []}

    def fake_process_one_resource(logical_name, resource_cfg, dataset_metadata):
        calls.append((logical_name, resource_cfg, dataset_metadata))
        return (
            Path(f"/tmp/{logical_name}_raw.parquet"),
            Path(f"/tmp/{logical_name}_prepared.parquet"),
        )

    monkeypatch.setattr("sirene.ingest.ensure_directories", fake_ensure_directories)
    monkeypatch.setattr(
        "sirene.ingest.fetch_dataset_metadata", fake_fetch_dataset_metadata
    )
    monkeypatch.setattr("sirene.ingest.process_one_resource", fake_process_one_resource)

    outputs = run()

    assert len(calls) == 2
    assert len(outputs) == 2
    assert calls[0][0] in {"unite_legale", "etablissement"}
    assert calls[1][0] in {"unite_legale", "etablissement"}
