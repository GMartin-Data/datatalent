WITH source AS (
    SELECT * FROM {{ source('geo', 'geo_communes') }}
),

departements AS (
    SELECT code, nom
    FROM {{ ref('stg_geo__departements') }}
),

regions AS (
    SELECT code, nom
    FROM {{ ref('stg_geo__regions') }}
)

SELECT
    source.code,
    source.nom,
    source.codeDepartement                  AS code_departement,
    departements.nom                        AS nom_departement,
    source.codeRegion                       AS code_region,
    regions.nom                             AS nom_region,
    source.population,
    source.centre.coordinates[OFFSET(1)]    AS centre_lat,
    source.centre.coordinates[OFFSET(0)]    AS centre_lon
FROM source
LEFT JOIN departements ON source.codeDepartement = departements.code
LEFT JOIN regions ON source.codeRegion = regions.code