-- ============================================================
-- Modèle : fct_offres
-- Couche : marts (matérialisé en `table` via dbt_project.yml — D52/D104)
--
-- Mart unifié des offres d'emploi IT/Data au grain offre, enrichi du
-- contexte territorial (densité communale URSSAF, tension départementale BMO,
-- benchmark salarial national URSSAF) et de métriques analytiques fenêtrées.
--
-- Position dans la chaîne (D94 — architecture mart unifié) :
--   int_offres_analytiques (couche analytique partagée, F1+F2+F5+F6)
--     → fct_offres (ce modèle, F3+F8+F9 + JOINs territoriaux + D75)
--
-- Sources amont :
--   - int_offres_analytiques (single source des offres + dérivées F1/F2/F5/F6)
--   - int_densite_sectorielle_commune (LEFT JOIN sur code_commune, snapshot D97)
--   - int_tensions_bassin_emploi (LEFT JOIN sur code_departement, snapshot D97)
--   - stg_urssaf__masse_salariale_na88 (CROSS JOIN benchmark national, snapshot D97)
--
-- Décisions associées :
--   - D94 : architecture mart unifié (racine `marts/` plate)
--   - D75 : statut_donnee + métadonnées de fiabilité (pct_couverture_*, is_fiable_*)
--   - D97 : snapshot temporel des dimensions contextuelles (MAX(annee) dynamique)
--   - D100 : F3 métriques fenêtrées
--   - D103 : F8 audit dbt
--   - D104 : F9 partitioning + clustering
--
-- Asymétries FT/Adzuna propagées (cf. int-offres-analytiques.md §2) :
--   - Adzuna : code_commune NULL → nb_offres_commune, densite_offres_par_100k_hab,
--     rang_volume_commune, nb_etablissements_it, effectifs_salaries_it NULL.
--   - Adzuna : code_departement ~100% renseigné → tension joignable.
--
-- Cardinalité préservée 1:1 vs int_offres_analytiques. Test
-- unique_combination_of_columns (source, offre_id) en filet de sécurité.
-- ============================================================

{{ config(
    partition_by={
        'field': 'date_creation',
        'data_type': 'timestamp',
        'granularity': 'month'
    },
    cluster_by=['categorie_metier', 'code_departement', 'source']
) }}

with source as (

    select * from {{ ref('int_offres_analytiques') }}

),

normalise_entreprise as (

    -- ========================================================
    -- CTE technique privée — normalisation entreprise_nom
    --
    -- Pattern EXCEPT(entreprise_nom_normalise) au SELECT final
    -- pour ne pas exposer la colonne technique (D100 A.6).
    -- Sert exclusivement à nb_offres_meme_entreprise_60j (D100 A.3).
    -- Normalisation locale au mart car non poussée en int_offres_analytiques.
    -- ========================================================
    select
        source.*,
        nullif(
            upper(
                trim(
                    regexp_replace(
                        coalesce(entreprise_nom, ''),
                        r'\s+',
                        ' '
                    )
                )
            ),
            ''
        ) as entreprise_nom_normalise

    from source

),

densite_snapshot as (

    -- ========================================================
    -- D97 A.1 — Snapshot densité communale (dernier millésime URSSAF effectifs)
    --
    -- Pattern MAX(annee) + INNER JOIN. Sans ce filtre, multiplication
    -- du grain offres par ~19 années URSSAF (2006-2024).
    -- ========================================================
    select d.*
    from {{ ref('int_densite_sectorielle_commune') }} as d
    inner join (
        select max(annee) as annee_max
        from {{ ref('int_densite_sectorielle_commune') }}
    ) as a
        on d.annee = a.annee_max

),

tension_snapshot as (

    -- ========================================================
    -- D97 A.1 — Snapshot tension départementale (dernier millésime BMO)
    --
    -- Pattern MAX(annee) + INNER JOIN. BMO ingéré 2025 actuellement.
    -- ========================================================
    select t.*
    from {{ ref('int_tensions_bassin_emploi') }} as t
    inner join (
        select max(annee) as annee_max
        from {{ ref('int_tensions_bassin_emploi') }}
    ) as a
        on t.annee = a.annee_max

),

salaire_benchmark as (

    -- ========================================================
    -- D97 A.1 — Snapshot benchmark salarial national (dernier millésime URSSAF masse salariale)
    --
    -- Pattern QUALIFY ROW_NUMBER() = 1 compact (source mono-ligne post-filtre).
    -- Piège typé : code_na88 est INT64 (cf. dbt-stg-urssaf-masse-salariale.md §2)
    -- → filtre = 62 sans quote, sinon type mismatch silencieux → CROSS JOIN
    -- sur 0 ligne → mart entier à 0 ligne.
    --
    -- Calcul salaire_brut_moyen_sectoriel au plus près de l'usage (D2 dbt
    -- rule : si une colonne calculée n'a qu'un seul usage downstream, calcul
    -- au plus près).
    -- ========================================================
    select
        annee as annee_contexte_salaire,
        round(masse_salariale_brute / effectifs_salaries_moyens, 0)
            as salaire_brut_moyen_sectoriel
    from {{ ref('stg_urssaf__masse_salariale_na88') }}
    where code_na88 = 62
    qualify row_number() over (order by annee desc) = 1

),

offres_contexte_territorial as (

    -- ========================================================
    -- Composition contextuelle (D97 A.2 — snapshot uniforme indépendant
    -- de annee_offre) :
    --   - LEFT JOIN densité sur code_commune (PLM symétrique 75056/13055/69123)
    --   - LEFT JOIN tension sur code_departement
    --   - CROSS JOIN salaire (1 ligne après snapshot)
    --
    -- Cardinalité 1:1 préservée par construction LEFT JOIN + snapshot 1:1
    -- + CROSS JOIN sur 1 ligne. Test unique_combination_of_columns en filet.
    -- ========================================================
    select
        o.*,

        -- Traçabilité année contexte (D97 A.2 — colonnes annee_contexte_*)
        extract(year from o.date_creation)                  as annee_offre,
        d.annee                                             as annee_contexte_densite,
        t.annee                                             as annee_contexte_tension,
        s.annee_contexte_salaire,

        -- Densité communale (NULL pour Adzuna par construction code_commune NULL)
        d.nb_etablissements_it,
        d.effectifs_salaries_it,

        -- Tension départementale
        t.projets_recrutement_it,
        t.projets_difficiles_it,
        t.part_difficile_pct_par_bassin,
        t.part_difficile_pct_par_projet,
        t.nb_bassins_total,
        t.nb_bassins_volume_observable,
        t.nb_bassins_taux_calculable,

        -- Benchmark salarial national IT (uniforme via CROSS JOIN)
        s.salaire_brut_moyen_sectoriel

    from normalise_entreprise as o
    left join densite_snapshot as d
        on o.code_commune = d.code_commune
    left join tension_snapshot as t
        on o.code_departement = t.code_departement
    cross join salaire_benchmark as s

),

fenetrees_base as (

    -- ========================================================
    -- D100 A.1-A.4 — Métriques fenêtrées (4/5 — sans rang)
    --
    -- COUNT OVER PARTITION BY sur les axes commune, métier×département,
    -- entreprise×fenêtre 60 jours glissants.
    --
    -- NULL explicite si la clé de partition est NULL (CASE WHEN) — sinon
    -- BigQuery groupe les NULLs ensemble dans une partition fantôme.
    -- ========================================================
    select
        o.*,

        -- D100 A.1 — Volume commune
        case
            when code_commune is null then null
            else count(*) over (partition by code_commune)
        end                                                 as nb_offres_commune,

        -- D100 A.2 — Volume métier × département
        case
            when categorie_metier is null
                or code_departement is null
                then null
            else count(*) over (
                partition by categorie_metier, code_departement
            )
        end                                                 as nb_offres_categorie_dept,

        -- D100 A.3 — Volume même entreprise sur 60 jours glissants
        -- Biais structurel : entreprise_nom à ~36.2% côté FT, NULL Adzuna.
        case
            when entreprise_nom_normalise is null
                or date_creation is null
                then null
            else count(*) over (
                partition by entreprise_nom_normalise
                order by unix_date(date(date_creation))
                range between 60 preceding and current row
            )
        end                                                 as nb_offres_meme_entreprise_60j,

        -- D100 A.4 — Densité offres par 100k habitants
        -- Dérivée de nb_offres_commune (non encore nommée à cette ligne du
        -- SELECT — recalcul de l'expression COUNT OVER nécessaire ici, sinon
        -- BigQuery refuse la référence à un alias de la même SELECT list).
        safe_divide(
            case
                when code_commune is null then null
                else count(*) over (partition by code_commune)
            end * 100000,
            nullif(cast(population as numeric), 0)
        )                                                   as densite_offres_par_100k_hab

    from offres_contexte_territorial as o

),

fenetrees_rang as (

    -- ========================================================
    -- D100 A.5 — Rang volume commune
    --
    -- DENSE_RANK global sur nb_offres_commune (calculé en CTE séparée car
    -- BigQuery interdit DENSE_RANK OVER (ORDER BY COUNT() OVER ()) imbriqué).
    -- NULL pour les offres sans code_commune (nb_offres_commune déjà NULL).
    -- ========================================================
    select
        fb.*,
        case
            when nb_offres_commune is null then null
            else dense_rank() over (order by nb_offres_commune desc)
        end                                                 as rang_volume_commune

    from fenetrees_base as fb

),

statut_qualite as (

    -- ========================================================
    -- D75 — Métadonnées de fiabilité tension (pct_couverture_*, is_fiable_*,
    -- statut_donnee).
    --
    -- Calculs dérivés des 3 compteurs nb_bassins_* exposés par
    -- int_tensions_bassin_emploi. Reprise intégrale du SQL
    -- fct_contexte_territorial (feature branch abandonnée).
    --
    -- statut_donnee caveat : 'masquée' agrège deux cas distincts (offre sans
    -- code_departement + département dont tous bassins BMO masqués). Choix
    -- volontaire, documenté en cadrage. Seuils is_fiable_* à 50%.
    -- ========================================================
    select
        fr.*,

        -- Pourcentages de couverture tension (NULL si offre non joinable)
        round(
            safe_divide(
                nb_bassins_volume_observable * 100.0,
                nb_bassins_total
            ),
            1
        )                                                   as pct_couverture_volume,

        round(
            safe_divide(
                nb_bassins_taux_calculable * 100.0,
                nb_bassins_total
            ),
            1
        )                                                   as pct_couverture_taux,

        -- Flags fiabilité — logique ternaire BOOL préservée (TRUE/FALSE/NULL)
        safe_divide(
            nb_bassins_volume_observable * 100.0,
            nb_bassins_total
        ) >= 50                                             as is_fiable_volume,

        safe_divide(
            nb_bassins_taux_calculable * 100.0,
            nb_bassins_total
        ) >= 50                                             as is_fiable_taux,

        -- Statut donnée pour choroplèthes Looker
        case
            when projets_recrutement_it is null then 'masquée'
            when projets_recrutement_it = 0   then 'zéro réel'
            else                                   'observée'
        end                                                 as statut_donnee

    from fenetrees_rang as fr

),

audite as (

    -- ========================================================
    -- D103 — F8 audit dbt
    --
    -- 2 colonnes run-volatile pour traçabilité (à quel run dbt cette ligne
    -- a-t-elle été produite, à quel instant). Localisation au mart (et non
    -- en int_offres_analytiques) — propriété run-volatile incompatible
    -- avec l'idempotence garantie de la couche analytique partagée.
    -- ========================================================
    select
        sq.*,
        current_timestamp()                                 as dbt_loaded_at,
        '{{ invocation_id }}'                               as dbt_invocation_id

    from statut_qualite as sq

)

select * except(entreprise_nom_normalise)
from audite