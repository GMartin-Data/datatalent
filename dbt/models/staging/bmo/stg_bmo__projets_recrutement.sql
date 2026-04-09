WITH source AS (
    SELECT *
    FROM {{ source('bmo', 'bmo')}}
)

SELECT
    annee,
    code_metier_bmo,
    libelle_metier_bmo,
    code_famille_metier,
    libelle_famille_metier,
    code_region,
    nom_region,
    code_departement,
    nom_departement,
    code_bassin_emploi,
    libelle_bassin_emploi,
    projets_recrutement,
    projets_difficiles,
    projets_saisonniers,

    CASE
        WHEN projets_recrutement IS NOT NULL
         AND projets_difficiles IS NOT NULL
         AND projets_recrutement > 0
        THEN ROUND(projets_difficiles * 100.0 / projets_recrutement, 1)
        ELSE NULL
    END AS part_difficile_pct

FROM source