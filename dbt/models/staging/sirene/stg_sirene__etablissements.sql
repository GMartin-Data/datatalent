{{ config(materialized='table') }}

WITH source AS (
    SELECT *
    FROM {{ source('sirene', 'sirene_etablissement') }}
    WHERE etatAdministratifEtablissement = 'A'
)

SELECT
    siret,
    siren,
    denominationUsuelleEtablissement        AS denomination_usuelle,
    enseigne1Etablissement                  AS enseigne,
    activitePrincipaleEtablissement         AS code_naf,
    trancheEffectifsEtablissement           AS tranche_effectifs,
    codeCommuneEtablissement                AS code_commune,
    codePostalEtablissement                 AS code_postal,
    libelleCommuneEtablissement             AS libelle_commune,
    dateCreationEtablissement               AS date_creation,
    etablissementSiege                      AS is_siege,
    (statutDiffusionEtablissement = 'P')    AS is_diffusion_partielle,
    (caractereEmployeurEtablissement = 'O') AS is_employeur

FROM source