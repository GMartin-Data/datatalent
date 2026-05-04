-- ============================================================
-- Modèle : stg_adzuna__offres
-- Couche : staging
-- Source : raw.adzuna
--
-- Objectif :
-- 1. Dédupliquer les offres Adzuna par offre_id
-- 2. Conserver la version la plus récente selon _ingestion_date
-- 3. Normaliser les types et calculer les colonnes utiles
-- 4. Enrichir les salaires avec une périodicité inférée
-- 5. Classifier les intitulés de poste
-- 6. Ajouter les flags métiers et conserver les colonnes salaire
--
-- Remarque :
-- Les REGEX ci-dessous sont provisoires.
-- Elles devront être alignées strictement avec France Travail
-- dès que la version finale commune sera validée.
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
    from {{ source('raw', 'adzuna') }}

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
    -- - la normalisation du titre pour les REGEX
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

        cast(salaire_est_estime as int64) as salaire_est_estime,

        -- -------------------------------
        -- Contrat / catégorie Adzuna
        -- -------------------------------
        type_contrat,
        temps_travail,
        categorie_tag,
        categorie_libelle,
        redirect_url,

        -- -------------------------------
        -- Titre normalisé pour les REGEX
        -- -------------------------------
        lower(titre) as titre_normalise

    from source_deduplicated

),

salary_enriched as (

    -- ========================================================
    -- Étape 4 : inférence prudente de la périodicité salaire
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
        end as salaire_periodicite_inferree,

        case
            when coalesce(salaire_min, salaire_max) between 15000 and 250000 then true
            when coalesce(salaire_min, salaire_max) between 1000 and 14999 then true
            else false
        end as is_salaire_periodicite_inferree,

        case
            when coalesce(salaire_min, salaire_max) between 15000 and 250000 then salaire_min
            when coalesce(salaire_min, salaire_max) between 1000 and 14999 then salaire_min * 12
            else null
        end as salaire_annuel_min,

        case
            when coalesce(salaire_min, salaire_max) between 15000 and 250000 then coalesce(salaire_max, salaire_min)
            when coalesce(salaire_min, salaire_max) between 1000 and 14999 then coalesce(salaire_max, salaire_min) * 12
            else null
        end as salaire_annuel_max

    from source_typed

),

classified as (

    -- ========================================================
    -- Étape 5 : classification métier à partir du titre
    --
    -- Version alignée au maximum avec France Travail
    -- + cas spécifiques observés dans Adzuna
    -- ========================================================
    select
        *,
        case
            -- ---------------------------
            -- Data Engineer
            -- aligné FT + cas spécifiques
            -- observés dans Adzuna
            -- ---------------------------
            when regexp_contains(
                titre_normalise,
                r'data\s+engineer|ing[eé]nieur\s+data|d[eé]veloppeur\s+data|data\s+engineering|ing[eé]nieur\s+de\s+donn[ée]es|data\s+ing[eé]nieur|dceo\s+engineer|data\s+cent(er|re)\s+engineering\s+operations'
            ) then 'data_engineer'

            -- ---------------------------
            -- Data Architect
            -- ---------------------------
            when regexp_contains(
                titre_normalise,
                r'architecte\s+data|data\s+architect|architecte\s+de\s+donn[ée]es'
            ) then 'data_architect'

            -- ---------------------------
            -- Data Analyst
            -- ---------------------------
            when regexp_contains(
                titre_normalise,
                r'data\s+analyst|analyste\s+data|business\s+data\s+analyst|data\s+product\s+analyst|analyste\s+de\s+donn[ée]es'
            ) then 'data_analyst'

            -- ---------------------------
            -- BI / décisionnel
            -- ---------------------------
            when regexp_contains(
                titre_normalise,
                r'(?:d[eé]veloppeur|analyste|consultant|ing[eé]nieur)\s+(?:bi|d[eé]cisionnel|business\s+intelligence)|(?:bi|d[eé]cisionnel|business\s+intelligence)\s+(?:d[eé]veloppeur|analyste|consultant|ing[eé]nieur)|power\s*bi|d[eé]veloppeur\s+bi|\bbi\b\s+(?:analyst|developer)'
            ) then 'bi_decisionnel'

            -- ---------------------------
            -- ML Engineer
            -- ---------------------------
            when regexp_contains(
                titre_normalise,
                r'mlops|ml\s+engineer|machine\s+learning\s+engineer|ing[eé]nieur\s+(machine\s+learning|ml)'
            ) then 'ml_engineer'

            -- ---------------------------
            -- Data Scientist
            -- ---------------------------
            when regexp_contains(
                titre_normalise,
                r'data\s+scientist'
            ) then 'data_scientist'

            -- ---------------------------
            -- Tous les autres cas
            -- ---------------------------
            else 'autre_it'
        end as categorie_metier

    from salary_enriched

),

final as (

    -- ========================================================
    -- Étape 6 : ajout des flags métiers finaux
    -- + conservation des colonnes salaire enrichies
    --
    -- Ce bloc ajoute :
    -- - is_metier_data
    -- - is_data_engineer
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

        case
            when categorie_metier != 'autre_it' then true
            else false
        end as is_metier_data,

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
        salaire_periodicite_inferree,
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