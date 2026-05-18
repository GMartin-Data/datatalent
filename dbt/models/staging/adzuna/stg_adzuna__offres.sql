-- ============================================================
-- Modèle : stg_adzuna__offres
-- Couche : staging
-- Source : raw.adzuna
--
-- Objectif :
-- 1. Dédupliquer les offres Adzuna par offre_id
-- 2. Conserver la version la plus récente selon _ingestion_date
-- 3. Normaliser les types et calculer les colonnes utiles
-- 4. Harmoniser les valeurs catégorielles Adzuna ↔ FT (D90)
-- 5. Enrichir les salaires avec une périodicité inférée
-- 6. Classifier les intitulés de poste via macro projet
-- 7. Ajouter les flags métiers et conserver les colonnes salaire
--
-- Remarque :
-- La classification métier est centralisée dans la macro
-- classify_categorie_metier afin d'éviter les divergences
-- entre Adzuna et France Travail.
-- ============================================================

with source_raw as (

    -- ========================================================
    -- Étape 1 : lecture brute de la source raw.adzuna
    -- + ajout d'un ROW_NUMBER pour dédupliquer les snapshots
    -- ========================================================
    select
        *,
        row_number() over (
            partition by offre_id
            order by _ingestion_date desc
        ) as _row_num
    from {{ source('adzuna', 'adzuna') }}

),

source_deduplicated as (

    -- ========================================================
    -- Étape 2 : conservation d'une seule ligne par offre_id
    -- On garde uniquement la version la plus récente
    -- ========================================================
    select *
    from source_raw
    where _row_num = 1

),

source_typed as (

    -- ========================================================
    -- Étape 3 : typage / normalisation des colonnes source
    --
    -- Ce bloc gère :
    -- - l'harmonisation de offre_id en STRING
    -- - les colonnes de géolocalisation
    -- - les colonnes salaire brutes
    -- - les colonnes contrat / catégorie Adzuna
    -- ========================================================
    select
        -- -------------------------------
        -- Identité de l'offre
        -- -------------------------------
        cast(offre_id as string) as offre_id,
        titre,
        description,
        date_creation,
        'adzuna' as source,

        -- -------------------------------
        -- Entreprise
        -- -------------------------------
        entreprise_nom,

        -- -------------------------------
        -- Localisation
        -- localisation_area est un champ
        -- répété, on extrait :
        -- [1] = région
        -- [2] = département
        -- -------------------------------
        localisation_libelle,
        localisation_area[safe_offset(1)] as region,
        localisation_area[safe_offset(2)] as departement_nom,
        cast(latitude as float64) as latitude,
        cast(longitude as float64) as longitude,

        -- -------------------------------
        -- Salaire brut Adzuna
        -- -------------------------------
        cast(salaire_min as float64) as salaire_min,
        cast(salaire_max as float64) as salaire_max,

        case
            when salaire_min is not null or salaire_max is not null then 'declare'
            else null
        end as source_salaire,

        safe_cast(salaire_est_estime as int64) as salaire_est_estime,

        -- -------------------------------
        -- Contrat / catégorie Adzuna
        -- -------------------------------
        type_contrat,
        temps_travail,
        categorie_tag,
        categorie_libelle,
        redirect_url

    from source_deduplicated

),

harmonized as (

    -- ========================================================
    -- Étape 4 : harmonisation des valeurs catégorielles
    --           Adzuna ↔ FT (D90, extension de D80/D86)
    --
    -- Mappings sémantiques au staging, pas en intermediate :
    -- - type_contrat : permanent → CDI, contract → CDD,
    --                  autre → NULL (asymétrie de couverture
    --                  Adzuna 2 codes vs FT 8 codes D88,
    --                  cf. accepted_values côté YAML)
    -- - temps_travail : full_time → Temps plein,
    --                   part_time → Temps partiel, autre → NULL
    --
    -- Le NULL sur valeur source inconnue est volontaire —
    -- pas de COALESCE masquant l'imprévu (parallèle d'esprit
    -- avec D75 sur le BMO).
    -- ========================================================
    select
        * except (type_contrat, temps_travail),

        case
            when type_contrat = 'permanent' then 'CDI'
            when type_contrat = 'contract' then 'CDD'
            else null
        end as type_contrat,

        case
            when temps_travail = 'full_time' then 'Temps plein'
            when temps_travail = 'part_time' then 'Temps partiel'
            else null
        end as temps_travail

    from source_typed

),

salary_enriched as (

    -- ========================================================
    -- Étape 5 : inférence prudente de la périodicité salaire
    --
    -- Règles retenues :
    -- - annuel : entre 15k et 250k
    -- - mensuel : entre 1k et 14 999
    -- - sinon : NULL
    --
    -- On n'infère pas l'horaire ici car trop ambigu pour Adzuna.
    -- ========================================================
    select
        *,

        case
            when coalesce(salaire_min, salaire_max) between 15000 and 250000 then 'annuel'
            when coalesce(salaire_min, salaire_max) between 1000 and 14999 then 'mensuel'
            else null
        end as salaire_periodicite,

        case
            when coalesce(salaire_min, salaire_max) between 15000 and 250000 then true
            when coalesce(salaire_min, salaire_max) between 1000 and 14999 then true
            else false
        end as is_salaire_periodicite_inferree,

        case
            when coalesce(salaire_min, salaire_max) between 15000 and 250000 then coalesce(salaire_min, salaire_max)
            when coalesce(salaire_min, salaire_max) between 1000 and 14999 then coalesce(salaire_min, salaire_max) * 12
            else null
        end as salaire_annuel_min,

        case
            when coalesce(salaire_min, salaire_max) between 15000 and 250000 then coalesce(salaire_max, salaire_min)
            when coalesce(salaire_min, salaire_max) between 1000 and 14999 then coalesce(salaire_max, salaire_min) * 12
            else null
        end as salaire_annuel_max

    from harmonized

),

classified as (

    -- ========================================================
    -- Étape 6 : classification métier à partir du titre
    --
    -- La logique est centralisée dans la macro projet
    -- classify_categorie_metier.
    -- ========================================================
    select
        *,
        {{ classify_categorie_metier('titre') }} as categorie_metier

    from salary_enriched

),

final as (

    -- ========================================================
    -- Étape 7 : ajout des flags métiers finaux
    -- + conservation des colonnes salaire enrichies
    --
    -- On ne filtre aucune ligne ici.
    -- Le tri sera fait plus tard en intermediate.
    -- ========================================================
    select
        -- -------------------------------
        -- Identité et description
        -- -------------------------------
        offre_id,
        titre,
        description,
        date_creation,
        source,

        -- -------------------------------
        -- Classification métier
        -- -------------------------------
        categorie_metier,

        (
            regexp_contains(lower(titre), r'data')
            or regexp_contains(lower(titre), r'ml|scientist|engineer|analyst')
        ) as is_metier_data,

        case
            when categorie_metier = 'data_engineer' then true
            else false
        end as is_data_engineer,

        -- -------------------------------
        -- Entreprise
        -- -------------------------------
        entreprise_nom,

        -- -------------------------------
        -- Localisation
        -- -------------------------------
        localisation_libelle,
        region,
        departement_nom,
        latitude,
        longitude,

        -- -------------------------------
        -- Salaire
        -- -------------------------------
        salaire_min,
        salaire_max,
        source_salaire,
        salaire_est_estime,
        salaire_periodicite,
        is_salaire_periodicite_inferree,
        salaire_annuel_min,
        salaire_annuel_max,

        -- -------------------------------
        -- Contrat / catégorie
        -- -------------------------------
        type_contrat,
        temps_travail,
        categorie_tag,
        categorie_libelle,
        redirect_url

    from classified

)

-- ============================================================
-- Résultat final du modèle de staging
-- ============================================================
select *
from final