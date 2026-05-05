WITH france_travail AS (

    SELECT
        offre_id,
        intitule AS titre,
        description,
        date_creation,
        source,
        categorie_metier,
        is_metier_data,

        -- Contrat
        type_contrat,
        experience_exige,
        experience_duree_annees,
        temps_travail,
        nombre_postes,
        is_alternance,
        qualification_code,
        qualification_libelle,

        -- Salaire
        salaire_annuel_min,
        salaire_annuel_max,
        is_salaire_aberrant,
        CASE
            WHEN salaire_annuel_min IS NOT NULL THEN 'declare'
            ELSE NULL
        END AS source_salaire,

        -- Géographie issue de l'offre
        code_commune,
        code_departement,
        libelle_lieu,
        latitude,
        longitude,

        -- Entreprise / secteur
        entreprise_nom,
        code_naf,
        secteur_activite_libelle,
        tranche_effectif,
        is_intermediaire,

        -- ROME
        rome_code,
        rome_libelle

    FROM {{ ref('stg_france_travail__offres') }}

),

departements_lookup AS (

    SELECT
        code,
        nom,
        nom_region
    FROM {{ ref('stg_geo__departements') }}

),

adzuna AS (

    SELECT
        a.offre_id,
        a.titre,
        a.description,
        a.date_creation,
        a.source,
        a.categorie_metier,
        a.is_metier_data,

        -- Contrat harmonisé
        CASE
            WHEN a.type_contrat = 'permanent' THEN 'CDI'
            WHEN a.type_contrat = 'contract' THEN 'CDD'
            ELSE NULL
        END AS type_contrat,
        CAST(NULL AS STRING) AS experience_exige,
        CAST(NULL AS FLOAT64) AS experience_duree_annees,
        CASE
            WHEN a.temps_travail = 'full_time' THEN 'Temps plein'
            WHEN a.temps_travail = 'part_time' THEN 'Temps partiel'
            ELSE NULL
        END AS temps_travail,
        CAST(NULL AS INT64) AS nombre_postes,
        CAST(NULL AS BOOLEAN) AS is_alternance,
        CAST(NULL AS STRING) AS qualification_code,
        CAST(NULL AS STRING) AS qualification_libelle,

        -- Salaire harmonisé
        a.salaire_annuel_min AS salaire_annuel_min,
        a.salaire_annuel_max AS salaire_annuel_max,
        CASE
            WHEN a.salaire_annuel_min IS NULL AND a.salaire_annuel_max IS NULL THEN NULL
            WHEN (a.salaire_annuel_min IS NOT NULL AND a.salaire_annuel_min < 15000)
              OR (a.salaire_annuel_max IS NOT NULL AND a.salaire_annuel_max > 150000)
            THEN TRUE
            ELSE FALSE
        END AS is_salaire_aberrant,
        a.source_salaire,

        -- Géographie issue de l'offre
        CAST(NULL AS STRING) AS code_commune,
        dept_lookup.code AS code_departement,
        a.localisation_libelle AS libelle_lieu,
        a.latitude,
        a.longitude,

        -- Entreprise / secteur
        a.entreprise_nom,
        CAST(NULL AS STRING) AS code_naf,
        CAST(NULL AS STRING) AS secteur_activite_libelle,
        CAST(NULL AS STRING) AS tranche_effectif,
        CAST(NULL AS BOOLEAN) AS is_intermediaire,

        -- ROME
        CAST(NULL AS STRING) AS rome_code,
        CAST(NULL AS STRING) AS rome_libelle

    FROM {{ ref('stg_adzuna__offres') }} AS a
    LEFT JOIN departements_lookup AS dept_lookup
        ON TRIM(LOWER(a.departement_nom)) = TRIM(LOWER(dept_lookup.nom))

),

offres_unifiees AS (

    SELECT * FROM france_travail
    UNION ALL
    SELECT * FROM adzuna

),

communes AS (

    SELECT
        code,
        nom,
        nom_departement,
        nom_region,
        population
    FROM {{ ref('stg_geo__communes') }}

),

departements_fallback AS (

    SELECT
        code,
        nom,
        nom_region
    FROM {{ ref('stg_geo__departements') }}

)

SELECT
    o.*,
    c.nom AS nom_commune,
    COALESCE(c.nom_departement, d.nom) AS nom_departement,
    COALESCE(c.nom_region, d.nom_region) AS nom_region,
    c.population
FROM offres_unifiees AS o
LEFT JOIN communes AS c
    ON o.code_commune = c.code
LEFT JOIN departements_fallback AS d
    ON o.code_departement = d.code