-- fct_contexte_territorial.sql

{{ config(materialized='table') }}

WITH derniere_annee_densite AS (
    SELECT MAX(annee) AS annee_max
    FROM {{ ref('int_densite_sectorielle_commune') }}
),

derniere_annee_tension AS (
    SELECT MAX(annee) AS annee_max
    FROM {{ ref('int_tensions_bassin_emploi') }}
),

densite_snapshot AS (
    SELECT d.*
    FROM {{ ref('int_densite_sectorielle_commune') }} d
    INNER JOIN derniere_annee_densite a ON d.annee = a.annee_max
),

tension_snapshot AS (
    SELECT t.*
    FROM {{ ref('int_tensions_bassin_emploi') }} t
    INNER JOIN derniere_annee_tension a ON t.annee = a.annee_max
),

salaire_dernier AS (
    SELECT
        annee AS annee_contexte_salaire,
        ROUND(masse_salariale_brute / effectifs_salaries_moyens, 0)
            AS salaire_brut_moyen_sectoriel
    FROM {{ ref('stg_urssaf__masse_salariale_na88') }}
    WHERE code_na88 = 62
    QUALIFY ROW_NUMBER() OVER (ORDER BY annee DESC) = 1
)

SELECT
    o.*,

    EXTRACT(YEAR FROM o.date_creation)        AS annee_offre,

    d.annee                                    AS annee_contexte_densite,
    d.nb_etablissements_it,
    d.effectifs_salaries_it,

    t.annee                                    AS annee_contexte_tension,
    t.projets_recrutement_it,
    t.projets_difficiles_it,
    t.part_difficile_pct_par_bassin,
    t.part_difficile_pct_par_projet,
    t.nb_bassins_total,
    t.nb_bassins_volume_observable,
    t.nb_bassins_taux_calculable,

    ROUND(SAFE_DIVIDE(t.nb_bassins_volume_observable * 100.0, t.nb_bassins_total), 1)
        AS pct_couverture_volume,
    ROUND(SAFE_DIVIDE(t.nb_bassins_taux_calculable * 100.0, t.nb_bassins_total), 1)
        AS pct_couverture_taux,
    SAFE_DIVIDE(t.nb_bassins_volume_observable * 100.0, t.nb_bassins_total) >= 50
        AS is_fiable_volume,
    SAFE_DIVIDE(t.nb_bassins_taux_calculable * 100.0, t.nb_bassins_total) >= 50
        AS is_fiable_taux,

    CASE
        WHEN t.projets_recrutement_it IS NULL THEN 'masquée'
        WHEN t.projets_recrutement_it = 0     THEN 'zéro réel'
        ELSE                                       'observée'
    END                                        AS statut_donnee,

    s.annee_contexte_salaire,
    s.salaire_brut_moyen_sectoriel

FROM {{ ref('int_offres_enrichies') }} o
LEFT JOIN densite_snapshot d
    ON o.code_commune = d.code_commune
LEFT JOIN tension_snapshot t
    ON o.code_departement = t.code_departement
CROSS JOIN salaire_dernier s
