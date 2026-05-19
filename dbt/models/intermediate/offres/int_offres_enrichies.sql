-- ============================================================
-- Modèle : int_offres_enrichies
-- Couche : intermediate (matérialisé en `table` via dbt_project.yml — D52)
--
-- Objectif :
-- 1. Unifier les offres France Travail et Adzuna via UNION ALL (D46)
-- 2. Résoudre le code_departement Adzuna (departement_nom → code)
-- 3. Enrichir avec les noms géographiques (commune, département,
--    région, population) via jointures stg_geo__communes (chemin
--    principal) et stg_geo__departements (fallback)
--
-- Principes (post-D90/D91) :
-- - Intermediate minimaliste. Toutes les harmonisations inter-sources
--   sont opérées au staging (D80 structure, D86 libellés/flags,
--   D90 valeurs, D91 is_salaire_aberrant Adzuna).
-- - Pas de filtrage de lignes — flags propagés pour filtrage en marts.
-- - Pas de déduplication inter-sources (D46).
-- - PLM transparent : remap arrondissements → commune centrale fait
--   au staging FT (D87), pas ici.
-- - SELECT explicites dans les CTE pré-UNION avec CAST(NULL AS <type>)
--   pour les colonnes absentes d'un côté.
-- - Pas de jointure stg_geo__regions : nom_region déjà dénormalisé
--   dans stg_geo__departements.
--
-- Voir int-offres-enrichies.md (spec composant) pour les détails.
-- ============================================================

with ft as (

    -- ========================================================
    -- CTE France Travail
    --
    -- Propagation pure depuis stg_france_travail__offres.
    -- CAST(NULL AS BOOLEAN) pour is_salaire_periodicite_inferree
    -- qui est Adzuna-only par construction (D86 A.4).
    -- ========================================================
    select
        -- Identité
        offre_id,
        titre,
        description,
        date_creation,
        source,

        -- Classification métier
        categorie_metier,
        is_metier_data,
        is_data_engineer,

        -- Contrat
        type_contrat,
        experience_exige,
        experience_duree_annees,
        temps_travail,
        nombre_postes,
        is_alternance,
        qualification_code,
        qualification_libelle,

        -- Salaire (normalisé annuel au staging — D86 A.5)
        salaire_annuel_min,
        salaire_annuel_max,
        is_salaire_aberrant,
        is_salaire_requalifie,
        cast(null as boolean)                       as is_salaire_periodicite_inferree,
        source_salaire,

        -- Géographie issue de l'offre (PLM remap fait au staging — D87)
        code_commune,
        code_departement,
        localisation_libelle                        as libelle_lieu,
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

    from {{ ref('stg_france_travail__offres') }}

),

adzuna as (

    -- ========================================================
    -- CTE Adzuna
    --
    -- Propagation depuis stg_adzuna__offres + résolution du
    -- code_departement via jointure textuelle sur le référentiel
    -- géographique des départements (égalité stricte — 0 non-match
    -- validé empiriquement 2026-05-18, YAGNI).
    --
    -- CAST(NULL AS <type>) pour les colonnes FT-only :
    -- experience_*, nombre_postes, is_alternance, qualification_*,
    -- code_commune, code_naf, secteur_*, tranche_effectif,
    -- is_intermediaire, is_salaire_requalifie, rome_*.
    -- ========================================================
    select
        -- Identité
        a.offre_id,
        a.titre,
        a.description,
        a.date_creation,
        a.source,

        -- Classification métier
        a.categorie_metier,
        a.is_metier_data,
        a.is_data_engineer,

        -- Contrat (déjà mappé au staging — D90)
        a.type_contrat,
        cast(null as string)                        as experience_exige,
        cast(null as float64)                       as experience_duree_annees,
        a.temps_travail,
        cast(null as int64)                         as nombre_postes,
        cast(null as boolean)                       as is_alternance,
        cast(null as string)                        as qualification_code,
        cast(null as string)                        as qualification_libelle,

        -- Salaire (inféré annuel au staging — D77 ; flag aligné — D91)
        a.salaire_annuel_min,
        a.salaire_annuel_max,
        a.is_salaire_aberrant,
        cast(null as boolean)                       as is_salaire_requalifie,
        a.is_salaire_periodicite_inferree,
        a.source_salaire,

        -- Géographie issue de l'offre
        cast(null as string)                        as code_commune,
        dept_lookup.code                            as code_departement,
        a.localisation_libelle                      as libelle_lieu,
        a.latitude,
        a.longitude,

        -- Entreprise / secteur
        a.entreprise_nom,
        cast(null as string)                        as code_naf,
        cast(null as string)                        as secteur_activite_libelle,
        cast(null as string)                        as tranche_effectif,
        cast(null as boolean)                       as is_intermediaire,

        -- ROME
        cast(null as string)                        as rome_code,
        cast(null as string)                        as rome_libelle

    from {{ ref('stg_adzuna__offres') }} as a
    left join {{ ref('stg_geo__departements') }} as dept_lookup
        on a.departement_nom = dept_lookup.nom

),

unioned as (

    -- ========================================================
    -- UNION ALL trivial — pas de déduplication inter-sources (D46).
    -- BigQuery valide au compile-time l'alignement strict des
    -- colonnes (nombre, ordre, types compatibles).
    -- ========================================================
    select * from ft
    union all
    select * from adzuna

),

enriched as (

    -- ========================================================
    -- Enrichissement géographique post-UNION
    --
    -- Chemin principal : LEFT JOIN stg_geo__communes via
    --   code_commune. Apporte nom_commune, nom_departement,
    --   nom_region, population (~92.7% FT, 0% Adzuna).
    --
    -- Chemin fallback : LEFT JOIN stg_geo__departements via
    --   code_departement. Apporte nom_departement et nom_region
    --   uniquement (granularité département). Cible Adzuna et
    --   les ~7.3% d'offres FT sans code_commune.
    --
    -- COALESCE priorise le chemin principal (richesse maximale).
    -- ========================================================
    select
        o.*,
        c.nom                                       as nom_commune,
        coalesce(c.nom_departement, d.nom)          as nom_departement,
        coalesce(c.nom_region, d.nom_region)        as nom_region,
        c.population                                as population
    from unioned as o
    left join {{ ref('stg_geo__communes') }}     as c
        on o.code_commune = c.code
    left join {{ ref('stg_geo__departements') }} as d
        on o.code_departement = d.code

)

select * from enriched