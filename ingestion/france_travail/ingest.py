"""Orchestration de l'ingestion France Travail — extract → JSON local → GCS → BigQuery.

La table raw.france_travail est partitionnée par _ingestion_date
(WRITE_APPEND — D19, D26).
"""

import datetime
import json
import os

import httpx
from google.cloud import bigquery
from shared.bigquery import load_gcs_to_bq
from shared.gcs import upload_to_gcs
from shared.logging import get_logger

from .client import FranceTravailClient
from .config import CODES_ROME, DEPARTEMENTS, OUTPUT_DIR

logger = get_logger(__name__)

FRANCE_TRAVAIL_SCHEMA = [
    # — Identité & métadonnées —
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("dateCreation", "TIMESTAMP"),
    bigquery.SchemaField("dateActualisation", "TIMESTAMP"),
    bigquery.SchemaField("intitule", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("romeCode", "STRING"),
    bigquery.SchemaField("romeLibelle", "STRING"),
    bigquery.SchemaField("appellationlibelle", "STRING"),
    bigquery.SchemaField("natureContrat", "STRING"),
    bigquery.SchemaField("typeContrat", "STRING"),
    bigquery.SchemaField("typeContratLibelle", "STRING"),
    bigquery.SchemaField("nombrePostes", "INTEGER"),
    # — Expérience —
    bigquery.SchemaField("experienceExige", "STRING"),
    bigquery.SchemaField("experienceLibelle", "STRING"),
    bigquery.SchemaField("experienceCommentaire", "STRING"),
    # — Conditions de travail —
    bigquery.SchemaField("alternance", "BOOLEAN"),
    bigquery.SchemaField("dureeTravailLibelle", "STRING"),
    bigquery.SchemaField("dureeTravailLibelleConverti", "STRING"),
    bigquery.SchemaField("complementExercice", "STRING"),
    bigquery.SchemaField("deplacementCode", "STRING"),  # ⚠ autodetect: INTEGER
    bigquery.SchemaField("deplacementLibelle", "STRING"),
    # — Qualification & secteur —
    bigquery.SchemaField("qualificationCode", "STRING"),  # ⚠ autodetect: INTEGER
    bigquery.SchemaField("qualificationLibelle", "STRING"),
    bigquery.SchemaField("secteurActivite", "STRING"),  # ⚠ autodetect: INTEGER
    bigquery.SchemaField("secteurActiviteLibelle", "STRING"),
    bigquery.SchemaField("codeNAF", "STRING"),
    bigquery.SchemaField("trancheEffectifEtab", "STRING"),
    # — Accessibilité —
    bigquery.SchemaField("accessibleTH", "BOOLEAN"),
    bigquery.SchemaField("entrepriseAdaptee", "BOOLEAN"),
    bigquery.SchemaField("employeurHandiEngage", "BOOLEAN"),
    bigquery.SchemaField("offresManqueCandidats", "BOOLEAN"),
    # — Lieu de travail —
    bigquery.SchemaField(
        "lieuTravail",
        "RECORD",
        fields=[
            bigquery.SchemaField("libelle", "STRING"),
            bigquery.SchemaField("commune", "STRING"),
            bigquery.SchemaField("codePostal", "STRING"),  # ⚠ autodetect: INTEGER
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
        ],
    ),
    # — Entreprise —
    bigquery.SchemaField(
        "entreprise",
        "RECORD",
        fields=[
            bigquery.SchemaField("nom", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("logo", "STRING"),
            bigquery.SchemaField("entrepriseAdaptee", "BOOLEAN"),
            bigquery.SchemaField("description", "STRING"),
        ],
    ),
    # — Salaire —
    bigquery.SchemaField(
        "salaire",
        "RECORD",
        fields=[
            bigquery.SchemaField("libelle", "STRING"),
            bigquery.SchemaField("commentaire", "STRING"),
            bigquery.SchemaField("complement1", "STRING"),
            bigquery.SchemaField("complement2", "STRING"),
            bigquery.SchemaField(
                "listeComplements",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("libelle", "STRING"),
                    bigquery.SchemaField("code", "STRING"),  # ⚠ autodetect: INTEGER
                ],
            ),
        ],
    ),
    # — Contact —
    bigquery.SchemaField(
        "contact",
        "RECORD",
        fields=[
            bigquery.SchemaField("nom", "STRING"),
            bigquery.SchemaField("courriel", "STRING"),
            bigquery.SchemaField("coordonnees1", "STRING"),
            bigquery.SchemaField("coordonnees2", "STRING"),
            bigquery.SchemaField("coordonnees3", "STRING"),
            bigquery.SchemaField("urlPostulation", "STRING"),
        ],
    ),
    # — Compétences, formations, langues, permis, qualités —
    bigquery.SchemaField(
        "competences",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("code", "STRING"),  # ⚠ autodetect: INTEGER
            bigquery.SchemaField("exigence", "STRING"),
            bigquery.SchemaField("libelle", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "formations",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("codeFormation", "STRING"),  # ⚠ autodetect: INTEGER
            bigquery.SchemaField("commentaire", "STRING"),
            bigquery.SchemaField("domaineLibelle", "STRING"),
            bigquery.SchemaField("exigence", "STRING"),
            bigquery.SchemaField("niveauLibelle", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "langues",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("exigence", "STRING"),
            bigquery.SchemaField("libelle", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "permis",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("exigence", "STRING"),
            bigquery.SchemaField("libelle", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "qualitesProfessionnelles",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("libelle", "STRING"),
        ],
    ),
    # — Contexte de travail —
    bigquery.SchemaField(
        "contexteTravail",
        "RECORD",
        fields=[
            bigquery.SchemaField("conditionsExercice", "STRING", mode="REPEATED"),
            bigquery.SchemaField("horaires", "STRING", mode="REPEATED"),
        ],
    ),
    # — Origine —
    bigquery.SchemaField(
        "origineOffre",
        "RECORD",
        fields=[
            bigquery.SchemaField("origine", "STRING"),  # ⚠ autodetect: INTEGER
            bigquery.SchemaField("urlOrigine", "STRING"),
            bigquery.SchemaField(
                "partenaires",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("url", "STRING"),
                    bigquery.SchemaField("logo", "STRING"),
                    bigquery.SchemaField("nom", "STRING"),
                ],
            ),
        ],
    ),
    # — Agence —
    bigquery.SchemaField(
        "agence",
        "RECORD",
        fields=[
            bigquery.SchemaField("courriel", "STRING"),
        ],
    ),
    # — Ingestion —
    bigquery.SchemaField("_ingestion_date", "DATE"),
]


def deduplicate_offres(offres: list[dict]) -> list[dict]:
    """Déduplique les offres par id (last-wins)."""
    seen = {}
    for offre in offres:
        seen[offre["id"]] = offre
    return list(seen.values())


def write_jsonl(offres: list[dict], file_path: str) -> None:
    """Écrit les offres au format JSONL (une ligne JSON par offre)."""
    with open(file_path, "w", encoding="utf-8") as f:
        for offre in offres:
            f.write(json.dumps(offre, ensure_ascii=False) + "\n")


def run():
    with FranceTravailClient(
        client_id=os.environ["FT_CLIENT_ID"],
        client_secret=os.environ["FT_CLIENT_SECRET"],
    ) as client:
        raw_offres = []

        for code in CODES_ROME:
            for dept in DEPARTEMENTS:
                logger.info(
                    "ingestion_start",
                    code_rome=code,
                    departement=dept,
                )
                try:
                    offres = client.fetch_offres(code, dept)
                except httpx.HTTPStatusError as e:
                    # Erreur non retryable (ex: 400) — log et skip pour ne pas perdre
                    # les données déjà collectées sur les autres combinaisons
                    logger.warning(
                        "fetch_skipped",
                        code_rome=code,
                        departement=dept,
                        status_code=e.response.status_code,
                    )
                    continue
                raw_offres.extend(offres)

        unique_offres = deduplicate_offres(raw_offres)
        logger.info(
            "dedup_complete",
            raw=len(raw_offres),
            unique=len(unique_offres),
        )

        today = str(datetime.date.today())
        for offre in unique_offres:
            offre["_ingestion_date"] = today

        filename = f"france_travail_{datetime.date.today().isoformat()}.jsonl"
        file_path = os.path.join(OUTPUT_DIR, filename)

        write_jsonl(unique_offres, file_path)
        logger.info(
            "file_written",
            path=file_path,
            count=len(unique_offres),
        )

    # Upload vers GCS
    gcs_uri = upload_to_gcs(file_path, "france_travail")
    logger.info("gcs_upload_complete", gcs_uri=gcs_uri)

    # Load dans BigQuery raw (WRITE_APPEND — D19)
    load_gcs_to_bq(
        gcs_uri,
        "raw",
        "france_travail",
        write_disposition="WRITE_APPEND",
        schema=FRANCE_TRAVAIL_SCHEMA,
        time_partitioning=bigquery.TimePartitioning(field="_ingestion_date"),
    )
    logger.info("bq_load_complete", table="raw.france_travail")

    logger.info("ingestion_end")


if __name__ == "__main__":
    import sys

    try:
        run()
    except Exception as exc:
        logger.exception("ingestion_failed", error=str(exc))
        sys.exit(1)
