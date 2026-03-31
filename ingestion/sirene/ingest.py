from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from shared.bigquery import load_gcs_to_bq
from shared.gcs import get_most_recent_blob_date, upload_to_gcs
from shared.logging import get_logger
from sirene.config import (
    CHUNK_SIZE,
    DATA_GOUV_API_DATASET_URL,
    GCS_PREFIX,
    HTTP_TIMEOUT_SECONDS,
    LOG_PROGRESS_INTERVAL_BYTES,
    RAW_DIR,
    SIRENE_RESOURCES,
    SKIP_IF_RECENT_DAYS,
)
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_logger(__name__)


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
    bq_table: str


def configure_logging() -> None:
    """Conserve un point d'entrée compatible avec les tests existants."""
    return None


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, max=10),
    reraise=True,
)
def fetch_dataset_metadata() -> dict[str, Any]:
    logger.info("dataset_metadata_fetch_started", url=DATA_GOUV_API_DATASET_URL)

    with httpx.Client(timeout=HTTP_TIMEOUT_SECONDS, follow_redirects=True) as client:
        response = client.get(DATA_GOUV_API_DATASET_URL)
        response.raise_for_status()
        payload = response.json()

    logger.info("dataset_metadata_fetch_succeeded")
    return payload


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
    """Construit un ResourceInfo à partir des métadonnées data.gouv et de la config.

    Args:
        logical_name: Identifiant logique de la ressource (ex: "unite_legale").
        resource_cfg: Configuration de la ressource depuis SIRENE_RESOURCES.
        dataset_metadata: Réponse JSON complète de l'API data.gouv.

    Returns:
        ResourceInfo hydraté avec les métadonnées et l'URL de téléchargement.

    Raises:
        ValueError: Si last_modified ou l'URL de téléchargement est absente.
    """
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
        bq_table=resource_cfg["bq_table"],
    )


def validate_resource_format(resource: ResourceInfo, expected_format: str) -> None:
    if resource.format != expected_format.lower():
        raise ValueError(
            f"Format inattendu pour {resource.logical_name}: "
            f"attendu={expected_format}, reçu={resource.format}"
        )


def build_month_tag(resource: ResourceInfo) -> str:
    return resource.last_modified.strftime("%Y-%m")


def build_raw_filename(resource: ResourceInfo) -> str:
    return f"{resource.filename_prefix}_{build_month_tag(resource)}.parquet"


def validate_parquet_magic_number(file_path: Path) -> None:
    with open(file_path, "rb") as file_handle:
        header = file_handle.read(4)

    if header != b"PAR1":
        raise ValueError(
            f"Le fichier téléchargé n'est pas un parquet valide "
            f"(magic number invalide) : {file_path}"
        )


def format_size(num_bytes: int | None) -> str:
    if num_bytes is None:
        return "taille inconnue"

    size = float(num_bytes)
    units = ["octets", "Ko", "Mo", "Go", "To"]
    unit_index = 0

    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    return f"{size:.2f} {units[unit_index]}"


def log_download_progress(
    downloaded_bytes: int, total_bytes: int | None, logical_name: str
) -> None:
    if total_bytes and total_bytes > 0:
        percent = (downloaded_bytes / total_bytes) * 100
        logger.info(
            "resource_download_progress",
            logical_name=logical_name,
            percent=round(percent, 1),
            downloaded_bytes=downloaded_bytes,
            total_bytes=total_bytes,
            downloaded_size=format_size(downloaded_bytes),
            total_size=format_size(total_bytes),
        )
    else:
        logger.info(
            "resource_download_progress",
            logical_name=logical_name,
            downloaded_bytes=downloaded_bytes,
            downloaded_size=format_size(downloaded_bytes),
            total_size="taille inconnue",
        )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, max=10),
    reraise=True,
)
def download_file(resource: ResourceInfo, destination: Path) -> None:
    """Télécharge un fichier Parquet en streaming avec écriture atomique.

    Écrit dans un fichier temporaire .part puis renomme en destination finale.
    Valide le magic number Parquet après téléchargement.

    Args:
        resource: Métadonnées de la ressource à télécharger.
        destination: Chemin local du fichier final.

    Raises:
        httpx.HTTPStatusError: Si le serveur retourne une erreur HTTP.
        ValueError: Si le fichier téléchargé n'est pas un Parquet valide.
    """
    temp_path = destination.with_suffix(destination.suffix + ".part")

    logger.info(
        "resource_download_started",
        logical_name=resource.logical_name,
        download_url=resource.download_url,
        destination=str(destination),
    )

    with httpx.Client(timeout=HTTP_TIMEOUT_SECONDS, follow_redirects=True) as client:
        with client.stream("GET", resource.download_url) as response:
            response.raise_for_status()

            content_length_header = response.headers.get("Content-Length")
            total_bytes = (
                int(content_length_header)
                if content_length_header and content_length_header.isdigit()
                else None
            )

            logger.info(
                "resource_size_announced",
                logical_name=resource.logical_name,
                total_bytes=total_bytes,
                total_size=format_size(total_bytes),
            )

            downloaded_bytes = 0
            next_log_threshold = LOG_PROGRESS_INTERVAL_BYTES
            first_log = True

            with open(temp_path, "wb") as file_handle:
                for chunk in response.iter_bytes(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue

                    file_handle.write(chunk)
                    downloaded_bytes += len(chunk)

                    if first_log or downloaded_bytes >= next_log_threshold:
                        log_download_progress(
                            downloaded_bytes, total_bytes, resource.logical_name
                        )
                        first_log = False
                        next_log_threshold = (
                            downloaded_bytes + LOG_PROGRESS_INTERVAL_BYTES
                        )

    temp_path.replace(destination)
    validate_parquet_magic_number(destination)

    logger.info(
        "resource_download_succeeded",
        logical_name=resource.logical_name,
        destination=str(destination),
        size_bytes=destination.stat().st_size,
        size=format_size(destination.stat().st_size),
    )


def process_one_resource(
    logical_name: str,
    resource_cfg: dict[str, str],
    dataset_metadata: dict[str, Any],
) -> str:
    """Traite une ressource Sirene : download → validate → upload GCS → load BQ.

    Args:
        logical_name: Identifiant logique (ex: "unite_legale", "etablissement").
        resource_cfg: Configuration depuis SIRENE_RESOURCES.
        dataset_metadata: Réponse JSON de l'API data.gouv.

    Returns:
        URI GCS du fichier uploadé.

    Raises:
        ValueError: Si le format ou le fichier téléchargé est invalide.
    """
    resource = build_resource_info(logical_name, resource_cfg, dataset_metadata)

    logger.info(
        "resource_processing_started",
        logical_name=resource.logical_name,
        title=resource.title,
        format=resource.format,
        last_modified=resource.last_modified.isoformat(),
        bq_table=resource.bq_table,
    )

    validate_resource_format(resource, resource_cfg["expected_format"])

    raw_filename = build_raw_filename(resource)
    raw_path = RAW_DIR / raw_filename

    download_file(resource, raw_path)

    if not raw_path.exists() or raw_path.stat().st_size == 0:
        raise ValueError(f"Le fichier téléchargé est absent ou vide : {raw_path}")

    logger.info(
        "local_file_ready",
        logical_name=resource.logical_name,
        local_path=str(raw_path),
        size_bytes=raw_path.stat().st_size,
        size=format_size(raw_path.stat().st_size),
    )

    gcs_uri = upload_to_gcs(str(raw_path), GCS_PREFIX)
    logger.info(
        "resource_uploaded_to_gcs",
        logical_name=resource.logical_name,
        gcs_uri=gcs_uri,
    )

    load_gcs_to_bq(gcs_uri, "raw", resource.bq_table)
    logger.info(
        "resource_loaded_to_bq",
        logical_name=resource.logical_name,
        gcs_uri=gcs_uri,
        bq_table=f"raw.{resource.bq_table}",
    )

    return gcs_uri


def _most_recent_blob_age_days() -> int | None:
    """Retourne l'âge en jours du blob le plus récent sous le prefix Sirene.

    Returns:
        Nombre de jours depuis la dernière mise à jour, ou None si aucun blob.
    """
    updated = get_most_recent_blob_date(GCS_PREFIX)
    if updated is None:
        return None
    age = datetime.now(UTC) - updated
    return age.days


def run() -> list[str]:
    """Point d'entrée de l'ingestion Sirene.

    Vérifie d'abord si un fichier récent existe dans GCS (D40).
    Si oui, skip l'ingestion. Sinon, traitement séquentiel normal.

    Returns:
        Liste des URIs GCS des fichiers uploadés (vide si skip).
    """
    configure_logging()
    ensure_directories()

    logger.info("sirene.ingestion_started")

    try:
        age_days = _most_recent_blob_age_days()
        if age_days is not None and age_days < SKIP_IF_RECENT_DAYS:
            logger.info(
                "sirene.skip_recent",
                age_days=age_days,
                threshold_days=SKIP_IF_RECENT_DAYS,
            )
            return []
    except Exception:
        logger.warning("sirene.skip_check_failed", exc_info=True)

    dataset_metadata = fetch_dataset_metadata()

    outputs: list[str] = []
    for logical_name, resource_cfg in SIRENE_RESOURCES.items():
        outputs.append(
            process_one_resource(logical_name, resource_cfg, dataset_metadata)
        )

    logger.info("sirene.ingestion_succeeded", resource_count=len(outputs))
    return outputs


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.exception("sirene.ingestion_failed", error=str(exc))
        sys.exit(1)
