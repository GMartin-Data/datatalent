from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import requests
from sirene.config import (
    CHUNK_SIZE,
    DATA_GOUV_API_DATASET_URL,
    HTTP_TIMEOUT_SECONDS,
    MAX_RESOURCE_AGE_DAYS,
    PREPARED_DIR,
    RAW_DIR,
    RESOURCE_SELECTED_COLUMNS,
    SIRENE_RESOURCES,
)


@dataclass
class ResourceInfo:
    logical_name: str
    resource_id: str
    title: str
    format: str
    mime: str | None
    last_modified: datetime
    download_url: str
    filename_prefix: str


def log(message: str) -> None:
    print(message, flush=True)


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PREPARED_DIR.mkdir(parents=True, exist_ok=True)


def fetch_dataset_metadata() -> dict[str, Any]:
    response = requests.get(DATA_GOUV_API_DATASET_URL, timeout=HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def parse_iso_datetime(value: str) -> datetime:
    cleaned = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def find_resource_by_id(
    dataset_metadata: dict[str, Any], resource_id: str
) -> dict[str, Any]:
    for resource in dataset_metadata.get("resources", []):
        if resource.get("id") == resource_id:
            return resource
    raise ValueError(f"Ressource introuvable pour resource_id={resource_id}")


def build_resource_info(
    logical_name: str,
    resource_cfg: dict[str, str],
    dataset_metadata: dict[str, Any],
) -> ResourceInfo:
    raw = find_resource_by_id(dataset_metadata, resource_cfg["resource_id"])

    resource_format = str(raw.get("format") or "").lower()
    resource_mime = raw.get("mime")
    resource_title = raw.get("title") or logical_name
    resource_last_modified = raw.get("last_modified")
    download_url = raw.get("latest") or raw.get("url")

    if not resource_last_modified:
        raise ValueError(
            f"La ressource {logical_name} ne contient pas de champ 'last_modified'."
        )

    if not download_url:
        raise ValueError(
            f"La ressource {logical_name} ne contient ni 'latest' ni 'url'."
        )

    return ResourceInfo(
        logical_name=logical_name,
        resource_id=resource_cfg["resource_id"],
        title=resource_title,
        format=resource_format,
        mime=resource_mime,
        last_modified=parse_iso_datetime(resource_last_modified),
        download_url=download_url,
        filename_prefix=resource_cfg["filename_prefix"],
    )


def validate_resource_format(resource: ResourceInfo, expected_format: str) -> None:
    if resource.format != expected_format.lower():
        raise ValueError(
            f"Format inattendu pour {resource.logical_name}: "
            f"attendu={expected_format}, reçu={resource.format}"
        )


def validate_resource_freshness(
    resource: ResourceInfo, max_age_days: int = MAX_RESOURCE_AGE_DAYS
) -> None:
    now_utc = datetime.now(UTC)
    age_days = (now_utc - resource.last_modified).days

    if age_days > max_age_days:
        raise ValueError(
            f"Ressource trop ancienne pour {resource.logical_name}: "
            f"{resource.last_modified.isoformat()} ({age_days} jours)."
        )


def build_month_tag(resource: ResourceInfo) -> str:
    return resource.last_modified.strftime("%Y-%m")


def build_raw_filename(resource: ResourceInfo) -> str:
    return f"{resource.filename_prefix}_{build_month_tag(resource)}.parquet"


def build_prepared_filename(resource: ResourceInfo) -> str:
    return f"{resource.filename_prefix}_{build_month_tag(resource)}_light.parquet"


def cleanup_old_versions(
    directory: Path, filename_prefix: str, keep_filename: str
) -> None:
    for file_path in directory.glob(f"{filename_prefix}_*.parquet"):
        if file_path.name != keep_filename and file_path.is_file():
            file_path.unlink()


def get_content_length(
    headers: requests.structures.CaseInsensitiveDict[str],
) -> int | None:
    raw_value = headers.get("Content-Length")
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except ValueError:
        return None


def download_file(resource: ResourceInfo, destination: Path) -> None:
    temp_path = destination.with_suffix(destination.suffix + ".part")

    with requests.get(
        resource.download_url, stream=True, timeout=HTTP_TIMEOUT_SECONDS
    ) as response:
        response.raise_for_status()

        final_url = response.url.lower()
        content_type = (response.headers.get("Content-Type") or "").lower()
        content_length = get_content_length(response.headers)

        if (
            not final_url.endswith(".parquet")
            and "parquet" not in content_type
            and "octet-stream" not in content_type
        ):
            raise ValueError(
                f"Le téléchargement reçu ne ressemble pas à un parquet : "
                f"url finale={response.url}, content-type={content_type}"
            )

        downloaded_bytes = 0
        with open(temp_path, "wb") as file_handle:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                file_handle.write(chunk)
                downloaded_bytes += len(chunk)

    if content_length is not None and downloaded_bytes != content_length:
        temp_path.unlink(missing_ok=True)
        raise ValueError(
            f"Téléchargement incomplet pour {resource.logical_name}: "
            f"{downloaded_bytes} octets reçus sur {content_length} attendus."
        )

    temp_path.replace(destination)


def select_existing_columns(
    available_columns: list[str], required_columns: list[str]
) -> list[str]:
    available_set = set(available_columns)
    return [col for col in required_columns if col in available_set]


def transform_parquet_keep_columns(
    source_path: Path,
    destination_path: Path,
    selected_columns: list[str],
) -> list[str]:
    """
    Seule opération de traitement :
    - lire le parquet source
    - garder uniquement les colonnes demandées si elles existent
    - réécrire un parquet allégé

    Aucune transformation métier sur les valeurs.
    """
    parquet_file = pq.ParquetFile(source_path)
    available_columns = parquet_file.schema_arrow.names
    kept_columns = select_existing_columns(available_columns, selected_columns)

    if not kept_columns:
        raise ValueError(
            f"Aucune colonne demandée n'a été trouvée dans {source_path.name}. "
            f"Colonnes demandées={selected_columns}"
        )

    temp_path = destination_path.with_suffix(destination_path.suffix + ".part")

    table = pq.read_table(source_path, columns=kept_columns)

    pq.write_table(
        table,
        temp_path,
        compression="zstd",
        use_dictionary=True,
    )

    temp_path.replace(destination_path)
    return kept_columns


def process_one_resource(
    logical_name: str,
    resource_cfg: dict[str, str],
    dataset_metadata: dict[str, Any],
) -> tuple[Path, Path]:
    resource = build_resource_info(logical_name, resource_cfg, dataset_metadata)

    log(f"\n=== {logical_name.upper()} ===")
    log(f"Titre ressource              : {resource.title}")
    log(f"Resource ID                  : {resource.resource_id}")
    log(f"Format                       : {resource.format}")
    log(f"Dernière modification API    : {resource.last_modified.isoformat()}")

    validate_resource_format(resource, resource_cfg["expected_format"])
    validate_resource_freshness(resource)

    raw_filename = build_raw_filename(resource)
    prepared_filename = build_prepared_filename(resource)

    raw_path = RAW_DIR / raw_filename
    prepared_path = PREPARED_DIR / prepared_filename

    log(f"Nom local raw                : {raw_filename}")
    log(f"Nom local prepared           : {prepared_filename}")
    log("Téléchargement du parquet brut...")

    download_file(resource, raw_path)

    if not raw_path.exists() or raw_path.stat().st_size == 0:
        raise ValueError(f"Le fichier brut téléchargé est absent ou vide : {raw_path}")

    requested_columns = RESOURCE_SELECTED_COLUMNS[logical_name]
    log(f"Sous-sélection des colonnes ({len(requested_columns)} colonnes demandées)...")

    kept_columns = transform_parquet_keep_columns(
        source_path=raw_path,
        destination_path=prepared_path,
        selected_columns=requested_columns,
    )

    if not prepared_path.exists() or prepared_path.stat().st_size == 0:
        raise ValueError(f"Le fichier préparé est absent ou vide : {prepared_path}")

    cleanup_old_versions(RAW_DIR, resource.filename_prefix, raw_path.name)
    cleanup_old_versions(PREPARED_DIR, resource.filename_prefix, prepared_path.name)

    raw_size_mb = raw_path.stat().st_size / (1024 * 1024)
    prepared_size_mb = prepared_path.stat().st_size / (1024 * 1024)

    log(f"Colonnes conservées          : {len(kept_columns)}")
    log(f"Taille raw                   : {raw_size_mb:.2f} Mo")
    log(f"Taille prepared              : {prepared_size_mb:.2f} Mo")
    log(f"Raw OK                       : {raw_path}")
    log(f"Prepared OK                  : {prepared_path}")

    if prepared_path.stat().st_size >= raw_path.stat().st_size:
        log(
            "[ATTENTION] Le parquet allégé reste plus lourd ou équivalent au brut. "
            "Cela peut venir de la stratégie de compression du fichier source. "
            "Le sous-ensemble de colonnes a bien été appliqué, mais la réécriture "
            "Parquet n'est pas forcément plus compacte que celle du fournisseur."
        )

    return raw_path, prepared_path


def run() -> list[tuple[Path, Path]]:
    ensure_directories()
    log("Récupération des métadonnées du dataset Sirene...")
    dataset_metadata = fetch_dataset_metadata()

    outputs: list[tuple[Path, Path]] = []
    for logical_name, resource_cfg in SIRENE_RESOURCES.items():
        outputs.append(
            process_one_resource(logical_name, resource_cfg, dataset_metadata)
        )

    log("\nExtraction et allègement terminés avec succès.")
    return outputs


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        log(f"\n[ERREUR] {exc}")
        sys.exit(1)
