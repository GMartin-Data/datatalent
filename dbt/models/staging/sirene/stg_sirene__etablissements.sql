{{ config(materialized='table') }}

WITH source AS (
    SELECT *
    FROM {{ source('sirene', 'sirene_etablissement') }}
    WHERE etatAdministratifEtablissement = 'A'
)

SELECT
    -- Identifiants
    siret,
    siren,

    -- Dénomination
    denominationUsuelleEtablissement        AS denomination_usuelle,
    enseigne1Etablissement                  AS enseigne,

    -- Activité et taille
    activitePrincipaleEtablissement         AS code_naf,
    trancheEffectifsEtablissement           AS tranche_effectifs,

    -- Géographie
    codeCommuneEtablissement                AS code_commune,
    codePostalEtablissement                 AS code_postal,
    libelleCommuneEtablissement             AS libelle_commune,

    -- Métadonnées
    dateCreationEtablissement               AS date_creation,
    etablissementSiege                      AS is_siege,
    (statutDiffusionEtablissement = 'P')    AS is_diffusion_partielle,
    (caractereEmployeurEtablissement = 'O') AS is_employeur

FROM source