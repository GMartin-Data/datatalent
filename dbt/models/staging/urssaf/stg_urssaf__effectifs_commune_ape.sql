-- stg_urssaf__effectifs_commune_ape.sql
-- Source : raw.urssaf_effectifs (format long après unpivot, WRITE_TRUNCATE)
-- Grain : une ligne = une commune × un code APE × une année
-- Transformation : COALESCE null → 0 sur les mesures (décision D39)
-- ⚠ Paris : codes arrondissements 75101–75120, pas commune centrale 75056
--   Ce décalage est géré en intermediate, pas ici.

WITH source AS (
    SELECT *
    FROM {{ source('urssaf', 'urssaf_effectifs') }}
)

-- _ingestion_date exclue : métadonnée technique, pas de valeur analytique pour cette source (spec §2)
SELECT
    code_commune,
    intitule_commune,
    code_departement,
    code_ape,
    annee,
    COALESCE(nb_etablissements, 0)  AS nb_etablissements,
    COALESCE(effectifs_salaries, 0) AS effectifs_salaries
FROM source