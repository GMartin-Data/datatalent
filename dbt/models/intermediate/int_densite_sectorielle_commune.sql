-- int_densite_sectorielle_commune.sql
-- Couche : intermediate
-- Grain  : une ligne par commune × année (post-normalisation PLM)
-- Source : stg_urssaf__effectifs_commune_ape

WITH source AS (

    SELECT *
    FROM {{ ref('stg_urssaf__effectifs_commune_ape') }}

),

normalized AS (

    SELECT
        -- Normalisation PLM : arrondissements → code central INSEE
        -- Sans ce CASE, Paris/Lyon/Marseille sont absents des jointures aval
        -- (FT utilise les codes centraux, URSSAF découpe par arrondissement)
        -- D74 — vérifié empiriquement sur BigQuery (SELECT DISTINCT code_commune)
        CASE
            WHEN code_commune BETWEEN '75101' AND '75120' THEN '75056'  -- Paris
            WHEN code_commune BETWEEN '69381' AND '69389' THEN '69123'  -- Lyon
            WHEN code_commune BETWEEN '13201' AND '13216' THEN '13055'  -- Marseille
            ELSE code_commune
        END AS code_commune,

        annee,
        nb_etablissements,
        effectifs_salaries

    FROM source

    -- Redondance documentée (D37) : le staging ne contient déjà que ces 4 codes.
    -- Rôle : protection contre élargissement futur du périmètre d'ingestion
    -- + lisibilité de l'intention métier dans le SQL (belt and suspenders).
    WHERE code_ape IN ('6201Z', '6202A', '6203Z', '6209Z')

)

SELECT
    code_commune,
    annee,
    SUM(nb_etablissements)  AS nb_etablissements_it,
    SUM(effectifs_salaries) AS effectifs_salaries_it

FROM normalized

GROUP BY
    code_commune,
    annee