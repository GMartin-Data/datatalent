-- int_tensions_bassin_emploi.sql
--
-- Agrégation départementale des projets de recrutement IT (BMO 2025).
-- Source unique : stg_bmo__projets_recrutement (6 codes FAP IT, M1X/M2X).
-- Grain de sortie : 1 ligne = département × année.
--
-- Le taux de difficulté est exposé sous deux formes complémentaires (D75) :
--   - méthode A (par bassin) : AVG des taux, vue territoriale
--   - méthode B-filtrée (par projet) : SUM/SUM aligné NULL-aware, vue volumique
-- Trois compteurs de couverture alimentent les flags is_fiable_* du mart aval.

WITH source AS (
    SELECT *
    FROM {{ ref('stg_bmo__projets_recrutement') }}
    WHERE code_metier_bmo LIKE 'M1X%'
       OR code_metier_bmo LIKE 'M2X%'
)

SELECT
    code_departement,
    annee,

    -- Volumes (agrégats avec propagation des NULLs du secret statistique)
    SUM(projets_recrutement)                AS projets_recrutement_it,
    SUM(projets_difficiles)                 AS projets_difficiles_it,

    -- Taux de difficulté — deux mesures complémentaires (D75)
    -- Méthode A : moyenne territoriale (chaque bassin = 1)
    ROUND(AVG(part_difficile_pct), 1)
        AS part_difficile_pct_par_bassin,

    -- Méthode B-filtrée : ratio volumique (chaque projet = 1)
    -- Le CASE aligne les périodes du numérateur et du dénominateur
    -- sur les bassins où les deux mesures sont renseignées.
    ROUND(
        SAFE_DIVIDE(
            SUM(projets_difficiles) * 100.0,
            SUM(CASE WHEN projets_difficiles IS NOT NULL
                     THEN projets_recrutement
                     END)
        ),
        1
    ) AS part_difficile_pct_par_projet,

    -- Métadonnées de couverture (alimentent les flags de fiabilité du mart)
    COUNT(*)                                AS nb_bassins_total,
    COUNT(projets_recrutement)              AS nb_bassins_volume_observable,
    COUNTIF(projets_difficiles IS NOT NULL) AS nb_bassins_taux_calculable

FROM source
GROUP BY code_departement, annee