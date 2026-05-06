-- models/marts/offres_enrichies/fct_offres_enrichies.sql
--
-- Mart principal des offres d'emploi au grain offre.
--
-- Grain :
--   1 ligne = 1 offre issue de int_offres_enrichies.
--
-- Objectif :
--   - éviter un simple pass-through depuis int_offres_enrichies
--   - exposer une table de faits stable pour Looker Studio
--   - matérialiser les règles métier dashboard :
--       F1 : salaires multi-vues
--       F2 : regroupement sectoriel codeNAF
--       F3 : métriques analytiques fenêtrées
--       F5 : flags qualité
--       F6 : dimensions temporelles
--       F8 : audit dbt
--
-- Remarque :
--   F4, dénormalisation avec mart_contexte_territorial, est volontairement
--   exclu de cette première version pour sécuriser le mart principal.

{{ config(
    materialized='table',
    partition_by={
        'field': 'date_creation',
        'data_type': 'timestamp',
        'granularity': 'month'
    },
    cluster_by=['categorie_metier', 'code_departement']
) }}

WITH offres_source AS (

    SELECT
        -- Identité offre
        offre_id,
        titre,
        description,
        date_creation,
        source,

        -- Métier
        categorie_metier,
        is_metier_data,
        type_contrat,
        experience_exige,
        experience_duree_annees,
        temps_travail,
        nombre_postes,
        is_alternance,
        qualification_code,
        qualification_libelle,
        rome_code,
        rome_libelle,

        -- Salaire
        salaire_annuel_min,
        salaire_annuel_max,
        is_salaire_aberrant,
        source_salaire,

        -- Géographie déjà enrichie dans int_offres_enrichies
        code_commune,
        code_departement,
        libelle_lieu,
        latitude,
        longitude,
        nom_commune,
        nom_departement,
        nom_region,
        population,

        -- Entreprise / secteur
        entreprise_nom,
        code_naf,
        secteur_activite_libelle,
        tranche_effectif,
        is_intermediaire,

        -- Normalisation du code NAF.
        -- Exemple :
        --   62.02A devient 6202A
        --   78.20Z devient 7820Z
        NULLIF(
            REGEXP_REPLACE(
                UPPER(TRIM(COALESCE(code_naf, ''))),
                r'[^0-9A-Z]',
                ''
            ),
            ''
        ) AS code_naf_normalise,

        -- Normalisation du nom entreprise pour les métriques recruteur.
        NULLIF(
            UPPER(
                TRIM(
                    REGEXP_REPLACE(
                        COALESCE(entreprise_nom, ''),
                        r'\s+',
                        ' '
                    )
                )
            ),
            ''
        ) AS entreprise_nom_normalise,

        -- Normalisation de la source salaire.
        LOWER(TRIM(COALESCE(source_salaire, ''))) AS source_salaire_normalisee

    FROM {{ ref('int_offres_enrichies') }}

),

offres_avec_salaire AS (

    SELECT
        s.*,

        -- F1 - Salaire milieu brut.
        -- Les salaires aberrants sont neutralisés.
        CASE
            WHEN COALESCE(is_salaire_aberrant, FALSE) = TRUE THEN NULL

            WHEN salaire_annuel_min IS NOT NULL
                AND salaire_annuel_max IS NOT NULL
                THEN (
                    SAFE_CAST(salaire_annuel_min AS NUMERIC)
                    + SAFE_CAST(salaire_annuel_max AS NUMERIC)
                ) / 2

            WHEN salaire_annuel_min IS NOT NULL
                THEN SAFE_CAST(salaire_annuel_min AS NUMERIC)

            WHEN salaire_annuel_max IS NOT NULL
                THEN SAFE_CAST(salaire_annuel_max AS NUMERIC)

            ELSE NULL
        END AS salaire_milieu,

        -- F1 - Salaire utilisable pour les agrégats.
        -- Les salaires estimés sont exclus si une telle valeur apparaît à l'avenir.
        CASE
            WHEN COALESCE(is_salaire_aberrant, FALSE) = TRUE THEN NULL

            WHEN source_salaire_normalisee IN (
                'estimé',
                'estime',
                'estimée',
                'estimee',
                'estimated'
            ) THEN NULL

            WHEN salaire_annuel_min IS NOT NULL
                AND salaire_annuel_max IS NOT NULL
                THEN (
                    SAFE_CAST(salaire_annuel_min AS NUMERIC)
                    + SAFE_CAST(salaire_annuel_max AS NUMERIC)
                ) / 2

            WHEN salaire_annuel_min IS NOT NULL
                THEN SAFE_CAST(salaire_annuel_min AS NUMERIC)

            WHEN salaire_annuel_max IS NOT NULL
                THEN SAFE_CAST(salaire_annuel_max AS NUMERIC)

            ELSE NULL
        END AS salaire_milieu_pour_agregat,

        -- F1 - Salaire agrégable hors intermédiaires.
        CASE
            WHEN COALESCE(is_salaire_aberrant, FALSE) = TRUE THEN NULL

            WHEN source_salaire_normalisee IN (
                'estimé',
                'estime',
                'estimée',
                'estimee',
                'estimated'
            ) THEN NULL

            WHEN COALESCE(is_intermediaire, FALSE) = TRUE THEN NULL

            WHEN salaire_annuel_min IS NOT NULL
                AND salaire_annuel_max IS NOT NULL
                THEN (
                    SAFE_CAST(salaire_annuel_min AS NUMERIC)
                    + SAFE_CAST(salaire_annuel_max AS NUMERIC)
                ) / 2

            WHEN salaire_annuel_min IS NOT NULL
                THEN SAFE_CAST(salaire_annuel_min AS NUMERIC)

            WHEN salaire_annuel_max IS NOT NULL
                THEN SAFE_CAST(salaire_annuel_max AS NUMERIC)

            ELSE NULL
        END AS salaire_milieu_hors_interim

    FROM offres_source AS s

),

offres_avec_secteur AS (

    SELECT
        s.*,

        -- F2 - Regroupement codeNAF en grandes catégories.
        CASE
            WHEN code_naf_normalise IS NULL THEN 'Non_renseigne'

            -- Section J : information et communication
            WHEN LEFT(code_naf_normalise, 2) IN ('62', '63') THEN 'Tech_IT_pur'
            WHEN LEFT(code_naf_normalise, 2) = '58' THEN 'Edition_logiciel'

            -- Section M : conseil et activités spécialisées
            WHEN code_naf_normalise IN (
                '7022Z',
                '7021Z',
                '7010Z',
                '7111Z',
                '7112B'
            ) THEN 'Conseil'

            -- Section N : intérim / activités de soutien
            WHEN LEFT(code_naf_normalise, 2) = '78' THEN 'ESN_Interim'
            WHEN code_naf_normalise IN ('8211Z', '8299Z') THEN 'ESN_Interim'

            -- Section K : banque, finance, assurance
            WHEN LEFT(code_naf_normalise, 2) IN ('64', '65', '66')
                THEN 'Banque_Finance_Assurance'

            -- Sections O, P, Q : public, éducation, santé, action sociale
            WHEN LEFT(code_naf_normalise, 2) IN ('84', '85', '86', '87', '88')
                THEN 'Public_Sante_Education'

            -- Sections C, F, G : industrie, construction, commerce
            WHEN LEFT(code_naf_normalise, 2) BETWEEN '10' AND '33'
                THEN 'Industrie_Commerce'
            WHEN LEFT(code_naf_normalise, 2) IN ('41', '42', '43')
                THEN 'Industrie_Commerce'
            WHEN LEFT(code_naf_normalise, 2) IN ('45', '46', '47')
                THEN 'Industrie_Commerce'

            ELSE 'Autres'
        END AS secteur_grande_categorie

    FROM offres_avec_salaire AS s

),

offres_enrichies_metier AS (

    SELECT
        s.*,

        -- F2 - Flag recruteur intermédiaire.
        (
            COALESCE(is_intermediaire, FALSE) = TRUE
            OR secteur_grande_categorie = 'ESN_Interim'
        ) AS est_recruteur_intermediaire,

        -- F1 - Tranches de salaire.
        CASE
            WHEN salaire_milieu IS NULL THEN 'non_renseigne'
            WHEN salaire_milieu < 35000 THEN 'a_moins_35k'
            WHEN salaire_milieu < 45000 THEN 'b_35_45k'
            WHEN salaire_milieu < 60000 THEN 'c_45_60k'
            WHEN salaire_milieu < 80000 THEN 'd_60_80k'
            WHEN salaire_milieu < 100000 THEN 'e_80_100k'
            ELSE 'f_plus_100k'
        END AS tranche_salaire,

        -- F1 - Flag salaire fiable.
        (
            salaire_milieu IS NOT NULL
            AND COALESCE(is_salaire_aberrant, FALSE) = FALSE
            AND COALESCE(is_intermediaire, FALSE) = FALSE
            AND (
                source_salaire IS NULL
                OR source_salaire_normalisee IN (
                    '',
                    'declare',
                    'déclaré',
                    'declaré',
                    'déclarée',
                    'declaree',
                    'declared'
                )
            )
        ) AS est_salaire_fiable,

        -- F5 - Score de complétude analytique, de 0 à 5.
        (
            CAST(salaire_milieu IS NOT NULL AS INT64)
            + CAST(code_commune IS NOT NULL AS INT64)
            + CAST(code_naf_normalise IS NOT NULL AS INT64)
            + CAST(entreprise_nom_normalise IS NOT NULL AS INT64)
            + CAST(LENGTH(COALESCE(description, '')) >= 200 AS INT64)
        ) AS qualite_donnee_score,

        -- F5 - Flags d'analysabilité par axe.
        (code_commune IS NOT NULL) AS est_analysable_geo_commune,
        (code_departement IS NOT NULL) AS est_analysable_geo_dept,
        (salaire_milieu IS NOT NULL) AS est_analysable_salaire,
        (code_naf_normalise IS NOT NULL) AS est_analysable_secteur,

        -- F6 - Dimensions temporelles.
        DATE(date_creation) AS date_creation_jour,
        DATE_TRUNC(DATE(date_creation), MONTH) AS mois_creation,
        EXTRACT(QUARTER FROM DATE(date_creation)) AS trimestre_creation,
        EXTRACT(YEAR FROM DATE(date_creation)) AS annee_creation,

        CASE
            WHEN date_creation IS NULL THEN NULL
            ELSE CONCAT(
                CAST(EXTRACT(YEAR FROM DATE(date_creation)) AS STRING),
                '-Q',
                CAST(EXTRACT(QUARTER FROM DATE(date_creation)) AS STRING)
            )
        END AS annee_trimestre_creation,

        DATE_DIFF(CURRENT_DATE(), DATE(date_creation), DAY)
            AS delai_jours_depuis_creation

    FROM offres_avec_secteur AS s

),

communes_volume AS (

    SELECT
        code_commune,
        COUNT(*) AS nb_offres_commune_total
    FROM offres_enrichies_metier
    WHERE code_commune IS NOT NULL
    GROUP BY code_commune

),

communes_rangs AS (

    SELECT
        code_commune,
        DENSE_RANK() OVER (
            ORDER BY nb_offres_commune_total DESC
        ) AS rang_volume_commune
    FROM communes_volume

),

offres_avec_metriques_base AS (

    SELECT
        o.*,

        -- F3 - Nombre d'offres dans la commune.
        CASE
            WHEN code_commune IS NULL THEN NULL
            ELSE COUNT(*) OVER (
                PARTITION BY code_commune
            )
        END AS nb_offres_commune,

        -- F3 - Nombre d'offres par catégorie métier et département.
        CASE
            WHEN categorie_metier IS NULL
                OR code_departement IS NULL
                THEN NULL
            ELSE COUNT(*) OVER (
                PARTITION BY categorie_metier, code_departement
            )
        END AS nb_offres_categorie_dept,

        -- F3 - Nombre d'offres d'une même entreprise sur 60 jours glissants.
        CASE
            WHEN entreprise_nom_normalise IS NULL
                OR date_creation IS NULL
                THEN NULL
            ELSE COUNT(*) OVER (
                PARTITION BY entreprise_nom_normalise
                ORDER BY UNIX_DATE(DATE(date_creation))
                RANGE BETWEEN 60 PRECEDING AND CURRENT ROW
            )
        END AS nb_offres_meme_entreprise_60j

    FROM offres_enrichies_metier AS o

),

offres_avec_metriques AS (

    SELECT
        o.*,

        -- F3 - Densité d'offres pour 100 000 habitants.
        SAFE_DIVIDE(
            CAST(o.nb_offres_commune AS NUMERIC) * 100000,
            NULLIF(CAST(o.population AS NUMERIC), 0)
        ) AS densite_offres_par_100k_hab,

        -- F3 - Rang de la commune selon le volume d'offres.
        r.rang_volume_commune,

        -- F8 - Audit dbt.
        CURRENT_TIMESTAMP() AS dbt_loaded_at,
        '{{ invocation_id }}' AS dbt_invocation_id

    FROM offres_avec_metriques_base AS o
    LEFT JOIN communes_rangs AS r
        USING (code_commune)

)

SELECT *
FROM offres_avec_metriques