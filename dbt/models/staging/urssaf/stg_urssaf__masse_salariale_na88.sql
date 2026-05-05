-- stg_urssaf__masse_salariale_na88.sql
-- Source : raw.urssaf_masse_salariale (JSONL, WRITE_TRUNCATE D19)
-- Grain : une ligne = une année (1998–2024)
-- Transformation : pass-through (colonnes renommées et typées à l'ingestion, D39)
-- Note : pas d'intermediate — jointure directe en mart sur code_na88 = 62

WITH source AS (
    SELECT *
    FROM {{ source('urssaf', 'urssaf_masse_salariale') }}
)

-- _ingestion_date exclue : métadonnée technique, pas de valeur analytique pour cette source (spec §2)
SELECT
    annee,
    code_na88,
    libelle_na88,
    nb_etablissements,
    effectifs_salaries_moyens,
    masse_salariale_brute
FROM source