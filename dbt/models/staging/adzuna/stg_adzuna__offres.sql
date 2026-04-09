-- ============================================================
-- Modèle : stg_adzuna__offres
-- Couche : staging
-- Source : raw.adzuna
--
-- Objectif :
-- 1. Dédupliquer les offres Adzuna par offre_id
-- 2. Conserver la version la plus récente selon _ingestion_date
-- 3. Normaliser les types et calculer les colonnes utiles
-- 4. Classifier les intitulés de poste
-- 5. Ajouter les flags métiers et salaire
--
-- Remarque :
-- Les REGEX ci-dessous sont provisoires.
-- Elles devront être alignées strictement avec France Travail
-- dès que tu me transmettras la version finale.
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
    -- - les colonnes salaire
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
        -- Salaire
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

classified as (

    -- ========================================================
    -- Étape 4 : classification métier à partir du titre
    --
    -- Cette version est provisoire.
    -- Elle sera remplacée par la logique exacte de France Travail
    -- pour garantir une parfaite cohérence inter-sources.
    -- ========================================================
    select
        *,
        case
            -- ---------------------------
            -- Data Engineer
            -- variantes anglaises + françaises
            -- + cas spécifiques observés dans Adzuna
            -- ---------------------------
            when regexp_contains(titre_normalise, r'\bdata\s*engineer\b')
                 or regexp_contains(titre_normalise, r'\bbig\s*data\s*engineer\b')
                 or regexp_contains(titre_normalise, r'\bcloud\s*data\s*engineer\b')
                 or regexp_contains(titre_normalise, r'\bdata\s*platform\s*engineer\b')
                 or regexp_contains(titre_normalise, r'\bing[eé]nieur\s*data\b')
                 or regexp_contains(titre_normalise, r'\bing[eé]nieur\s*de\s*donn[ée]es\b')
                 or regexp_contains(titre_normalise, r'\bing[eé]nieur\s*des\s*donn[ée]es\b')
                 or regexp_contains(titre_normalise, r'\bdceo\s*engineer\b')
                 or regexp_contains(titre_normalise, r'\bdata\s*cent(er|re)\s*engineering\s*operations\b')
            then 'data_engineer'

            -- ---------------------------
            -- Data Architect
            -- ---------------------------
            when regexp_contains(titre_normalise, r'\bdata\s*architect\b')
                 or regexp_contains(titre_normalise, r'\barchitecte\s*data\b')
                 or regexp_contains(titre_normalise, r'\barchitecte\s*de\s*donn[ée]es\b')
            then 'data_architect'

            -- ---------------------------
            -- ML Engineer
            -- ---------------------------
            when regexp_contains(titre_normalise, r'\b(machine\s*learning|ml)\s*engineer\b')
                 or regexp_contains(titre_normalise, r'\bing[eé]nieur\s*(machine\s*learning|ml)\b')
            then 'ml_engineer'

            -- ---------------------------
            -- Data Scientist
            -- ---------------------------
            when regexp_contains(titre_normalise, r'\bdata\s*scientist\b')
                 or regexp_contains(titre_normalise, r'\bscientist\s*data\b')
            then 'data_scientist'

            -- ---------------------------
            -- Data Analyst
            -- ---------------------------
            when regexp_contains(titre_normalise, r'\bdata\s*anal')
                 or regexp_contains(titre_normalise, r'\banalyste\s*data\b')
                 or regexp_contains(titre_normalise, r'\banalyste\s*de\s*donn[ée]es\b')
            then 'data_analyst'

            -- ---------------------------
            -- BI / décisionnel
            -- ---------------------------
            when regexp_contains(titre_normalise, r'\bbusiness\s*intelligence\b')
                 or regexp_contains(titre_normalise, r'\bd[eé]cisionnel\b')
            then 'bi_decisionnel'

            -- ---------------------------
            -- Tous les autres cas
            -- ---------------------------
            else 'autre_it'
        end as categorie_metier

    from source_typed

),

final as (

    -- ========================================================
    -- 5. Ajouter les flags métiers et conserver les colonnes salaire
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