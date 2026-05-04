WITH source AS (
    SELECT * FROM {{ source('geo', 'geo_departements') }}
),

regions AS (
    SELECT code, nom
    FROM {{ ref('stg_geo__regions') }}
)

SELECT
    source.code,
    source.nom,
    source.codeRegion   AS code_region,
    regions.nom         AS nom_region,
    source.zone         AS zone_geo
FROM source
LEFT JOIN regions ON source.codeRegion = regions.code