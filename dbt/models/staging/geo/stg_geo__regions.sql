WITH source AS (

    SELECT * FROM {{ source('geo', 'geo_regions') }}

)

SELECT
    code,
    nom,
    zone AS zone_geo
FROM source