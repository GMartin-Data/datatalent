-- ============================================================
-- Modèle : int_offres_analytiques
-- Couche : intermediate (matérialisé en `table` via dbt_project.yml — D52)
--
-- Objectif :
-- Couche analytique partagée (D95) — héberge les dérivées métier
-- au grain offre exploitables en aval par `fct_offres` :
--   F1 — Salaires multi-vues (D98)
--   F2 — Regroupement codeNAF (D99)
--   F5 — Flags qualité (D101)
--   F6 — Dimensions temporelles (D102)
--
-- Position dans la chaîne :
--   int_offres_enrichies (Silver conforming, D80 minimaliste)
--     → int_offres_analytiques (couche analytique partagée, ce modèle)
--       → fct_offres (mart de présentation, héberge F3+F8+F9)
--
-- Inversion D80 → D95 documentée : cette couche introduit
-- intentionnellement des dérivées métier dans l'intermediate, vs le
-- principe Silver conforming initial. Justification : factorisation
-- des transformations partagées entre `fct_offres` actuel et futurs
-- consommateurs (analyses ad-hoc, futurs marts).
--
-- F3 (métriques fenêtrées), F8 (audit dbt) et F9 (partitioning) sont
-- hébergées dans `fct_offres` (couche mart, run-volatility tolérée).
-- F4 (dénormalisation cross-mart) est obsolète par construction post-D94+D96.
-- F7 (géographie sémantique) est hors scope MVP.
--
-- Idempotence garantie (D102 A.4) : aucune référence à CURRENT_DATE()
-- ou CURRENT_TIMESTAMP(). Même source → même output run après run.
--
-- Asymétries FT/Adzuna acceptées par construction (cf. D98, D99, D101) :
--   - Adzuna : code_commune et code_naf 100% NULL → est_analysable_geo_commune
--     et est_analysable_secteur systématiquement FALSE → score F5 plafonné à 3/5.
--   - FT : entreprise_nom 36.2% couverture, is_salaire_periodicite_inferree
--     NULL (Adzuna-only).
--
-- Voir int-offres-analytiques.md (spec composant) pour les détails.
-- ============================================================

with source as (

    select * from {{ ref('int_offres_enrichies') }}

),

salaires_calcules as (

    -- ========================================================
    -- F1 (technique) — Pré-calcul de salaire_milieu brut
    --
    -- CTE technique non exposée hors F1. Factorise le calcul de
    -- salaire_milieu réutilisé par 3 autres colonnes F1 et par
    -- tranche_salaire. Pattern EXCEPT(_salaire_milieu_calc) en
    -- aval pour garder la colonne privée.
    --
    -- A.2 (D98) : fallback unilatéral si min XOR max est NULL.
    -- ========================================================
    select
        source.*,
        case
            when salaire_annuel_min is not null and salaire_annuel_max is not null
                then (salaire_annuel_min + salaire_annuel_max) / 2.0
            when salaire_annuel_min is not null then salaire_annuel_min
            when salaire_annuel_max is not null then salaire_annuel_max
            else null
        end as _salaire_milieu_calc

    from source

),

salaires_multivues as (

    -- ========================================================
    -- F1 — Salaires multi-vues (D98)
    --
    -- 5 colonnes dérivées au grain offre :
    --   - salaire_milieu : vue brute (A.2 fallback unilatéral)
    --   - salaire_milieu_pour_agregat : exclut estimés (D48) + aberrants (D91)
    --   - salaire_milieu_hors_interim : + intermédiaires (D14-bis)
    --   - tranche_salaire : 7 buckets STRING
    --   - est_salaire_fiable : flag composite qualité maximale (A.4)
    --
    -- Invariance par construction CASE : _hors_interim ⊂ _pour_agregat ⊂ salaire_milieu
    -- ========================================================
    select
        salaires_calcules.* except(_salaire_milieu_calc),

        _salaire_milieu_calc                                as salaire_milieu,

        case
            when source_salaire = 'estime' then null
            when coalesce(is_salaire_aberrant, false) then null
            else _salaire_milieu_calc
        end                                                 as salaire_milieu_pour_agregat,

        case
            when source_salaire = 'estime' then null
            when coalesce(is_salaire_aberrant, false) then null
            when coalesce(is_intermediaire, false) then null
            else _salaire_milieu_calc
        end                                                 as salaire_milieu_hors_interim,

        case
            when _salaire_milieu_calc is null then 'non_renseigne'
            when _salaire_milieu_calc < 35000 then 'a_moins_35k'
            when _salaire_milieu_calc < 45000 then 'b_35_45k'
            when _salaire_milieu_calc < 60000 then 'c_45_60k'
            when _salaire_milieu_calc < 80000 then 'd_60_80k'
            when _salaire_milieu_calc < 100000 then 'e_80_100k'
            else 'f_plus_100k'
        end                                                 as tranche_salaire,

        (
            (source_salaire = 'declare' or source_salaire is null)
            and coalesce(is_intermediaire, false) = false
            and coalesce(is_salaire_aberrant, false) = false
            and (salaire_annuel_min is not null or salaire_annuel_max is not null)
        )                                                   as est_salaire_fiable

    from salaires_calcules

),

secteur_categorise as (

    -- ========================================================
    -- F2 — Regroupement codeNAF (D99)
    --
    -- 2 colonnes dérivées au grain offre :
    --   - secteur_grande_categorie : 9 buckets STRING
    --   - est_recruteur_intermediaire : OR is_intermediaire / NACE 78
    --
    -- A.3 — Présuppose code_naf sans point (convention API FT/URSSAF).
    --   Vérification au spot check 4.
    -- A.5 — Asymétrie : Adzuna code_naf NULL → systématiquement
    --   Non_renseigne + est_recruteur_intermediaire = FALSE.
    -- ========================================================
    select
        salaires_multivues.*,

        case
            when code_naf is null or trim(code_naf) = '' then 'Non_renseigne'

            -- Section J — Information et communication (cœur IT)
            when left(code_naf, 2) in ('62', '63') then 'Tech_IT_pur'
            when left(code_naf, 2) = '58' then 'Edition_logiciel'

            -- Section M — Activités spécialisées (conseil)
            when code_naf in ('70.22Z', '70.21Z', '70.10Z', '71.11Z', '71.12B') then 'Conseil'

            -- Section N — Activités de soutien (ESN/intérim)
            when left(code_naf, 2) = '78' then 'ESN_Interim'
            when code_naf in ('82.11Z', '82.99Z') then 'ESN_Interim'

            -- Section K — Activités financières et d'assurance
            when left(code_naf, 2) in ('64', '65', '66') then 'Banque_Finance_Assurance'

            -- Sections O, P, Q — Public, éducation, santé
            when left(code_naf, 2) in ('84', '85', '86') then 'Public_Sante_Education'

            -- Sections C, F, G — Industrie, construction, commerce
            when left(code_naf, 2) between '10' and '33' then 'Industrie_Commerce'
            when left(code_naf, 2) in ('45', '46', '47') then 'Industrie_Commerce'
            when left(code_naf, 2) in ('41', '42', '43') then 'Industrie_Commerce'

            else 'Autres'
        end                                                 as secteur_grande_categorie,

        (
            coalesce(is_intermediaire, false) = true
            or left(coalesce(code_naf, ''), 2) = '78'
        )                                                   as est_recruteur_intermediaire

    from salaires_multivues

),

flags_qualite as (

    -- ========================================================
    -- F5 — Flags qualité (D101)
    --
    -- 5 colonnes dérivées au grain offre :
    --   - qualite_donnee_score : 0-5 (somme analysabilités + critère description)
    --   - est_analysable_geo_commune : code_commune renseigné
    --   - est_analysable_geo_dept : code_departement renseigné
    --   - est_analysable_salaire : alias direct de est_salaire_fiable (A.2)
    --   - est_analysable_secteur : secteur F2 hors Non_renseigne (A.3)
    --
    -- Score = indicateur de richesse d'analysabilité, non de qualité
    -- intrinsèque de l'offre. Adzuna plafonné à 3/5 par construction.
    -- ========================================================
    select
        secteur_categorise.*,

        est_salaire_fiable                                  as est_analysable_salaire,
        (code_commune is not null)                          as est_analysable_geo_commune,
        (code_departement is not null)                      as est_analysable_geo_dept,
        (secteur_grande_categorie != 'Non_renseigne')       as est_analysable_secteur,

        (
            cast(est_salaire_fiable as int64)
            + cast(code_commune is not null as int64)
            + cast(secteur_grande_categorie != 'Non_renseigne' as int64)
            + cast(entreprise_nom is not null as int64)
            + cast(length(coalesce(description, '')) >= 200 as int64)
        )                                                   as qualite_donnee_score

    from secteur_categorise

),

dimensions_temporelles as (

    -- ========================================================
    -- F6 — Dimensions temporelles (D102)
    --
    -- 5 colonnes dérivées au grain offre, toutes déterministes en
    -- fonction de date_creation seule :
    --   - date_creation_jour : DATE
    --   - mois_creation : DATE (premier jour du mois)
    --   - trimestre_creation : INT64 (1-4)
    --   - annee_creation : INT64
    --   - annee_trimestre_creation : STRING ('2026-Q2')
    --
    -- A.4 — Idempotence garantie. delai_jours_depuis_creation exclu
    --   (option c) — calculable côté Looker en calculated field.
    -- ========================================================
    select
        flags_qualite.*,

        date(date_creation)                                 as date_creation_jour,
        date_trunc(date(date_creation), month)              as mois_creation,
        extract(quarter from date_creation)                 as trimestre_creation,
        extract(year from date_creation)                    as annee_creation,
        format_date('%Y-Q%Q', date(date_creation))          as annee_trimestre_creation

    from flags_qualite

)

select * from dimensions_temporelles