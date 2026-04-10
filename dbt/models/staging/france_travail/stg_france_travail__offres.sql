-- models/staging/france_travail/stg_france_travail__offres.sql

with source as (
    select * 
    from {{ source('france_travail_raw', 'france_travail') }}
),

renamed as (
    select
        -- 1. Identité
        id as offre_id,
        TIMESTAMP(dateCreation) as date_creation,
        TIMESTAMP(dateActualisation) as date_actualisation,
        intitule,
        description,
        romeCode as rome_code,
        romeLibelle as rome_libelle,
        CAST(nombrePostes AS INT64) as nombre_postes,
        'france_travail' as source,

        -- 2. Contrat
        typeContrat as type_contrat,
        typeContratLibelle as type_contrat_libelle,
        experienceExige as experience_exige,
        experienceLibelle as experience_libelle,
        dureeTravailLibelleConverti as temps_travail,
        CAST(alternance AS BOOLEAN) as is_alternance,
        qualificationCode as qualification_code,
        qualificationLibelle as qualification_libelle,

        -- 3. Salaire
        salaire.libelle as salaire_libelle,
        salaire.commentaire as salaire_commentaire,
        LOWER(REGEXP_EXTRACT(salaire.libelle, r'(Annuel|Mensuel|Horaire)')) as salaire_periodicite,
        SAFE_CAST(REGEXP_EXTRACT(salaire.libelle, r'(?:Annuel|Mensuel|Horaire)\s+de\s+([\d.]+)') AS FLOAT64) as salaire_min_euros,
        SAFE_CAST(REGEXP_EXTRACT(salaire.libelle, r'à\s+([\d.]+)\s+Euros') AS FLOAT64) as salaire_max_euros,
        SAFE_CAST(REGEXP_EXTRACT(salaire.libelle, r'sur\s+([\d.]+)\s+mois') AS FLOAT64) as salaire_nb_mois,

        -- 4. Géographie
        CASE 
            -- Paris : de 75101 à 75120 -> 75056
            WHEN LPAD(CAST(lieuTravail.commune AS STRING), 5, '0') BETWEEN '75101' AND '75120' THEN '75056'
            -- Marseille : de 13201 à 13216 -> 13055
            WHEN LPAD(CAST(lieuTravail.commune AS STRING), 5, '0') BETWEEN '13201' AND '13216' THEN '13055'
            -- Lyon : de 69381 à 69389 -> 69123
            WHEN LPAD(CAST(lieuTravail.commune AS STRING), 5, '0') BETWEEN '69381' AND '69389' THEN '69123'
            -- Le reste des communes
            ELSE LPAD(CAST(lieuTravail.commune AS STRING), 5, '0')
        END as code_commune,


        lieuTravail.libelle as libelle_lieu,
        CAST(lieuTravail.latitude AS FLOAT64) as latitude,
        CAST(lieuTravail.longitude AS FLOAT64) as longitude,

        -- 5. Secteur
        codeNAF as code_naf,
        secteurActiviteLibelle as secteur_activite_libelle,
        trancheEffectifEtab as tranche_effectif,
        entreprise.nom as entreprise_nom,

        _ingestion_date
    from source
),

deduplicated as (
    select *
    from renamed
    qualify row_number() over (
        partition by offre_id
        order by _ingestion_date desc
    ) = 1
),

with_metier as (
    select *,
        -- Classification métier
        case
            when REGEXP_CONTAINS(intitule, r'data\s+engineer|ing[eé]nieur\s+data|d[eé]veloppeur\s+data|data\s+engineering|ing[eé]nieur\s+de\s+donn[ée]es|data\s+ing[eé]nieur|dceo\s+engineer|data\s+cent(er|re)\s+engineering\s+operations') then 'data_engineer'
            when REGEXP_CONTAINS(intitule, r'architecte\s+data|data\s+architect|architecte\s+de\s+donn[ée]es') then 'data_architect'
            
            when REGEXP_CONTAINS(intitule, r'data\s+analyst|analyste\s+data|business\s+data\s+analyst|data\s+product\s+analyst|analyste\s+de\s+donn[ée]es') then 'data_analyst'
            when REGEXP_CONTAINS(intitule, r'(?:d[eé]veloppeur|analyste|consultant|ing[eé]nieur)\s+(?:bi|d[eé]cisionnel|business\s+intelligence)|(?:bi|d[eé]cisionnel|business\s+intelligence)\s+(?:d[eé]veloppeur|analyste|consultant|ing[eé]nieur)|power\s*bi|d[eé]veloppeur\s+bi|\bbi\b\s+(?:analyst|developer)') then 'bi_decisionnel'
            when REGEXP_CONTAINS(intitule, r'mlops|ml\s+engineer|machine\s+learning\s+engineer|ing[eé]nieur\s+(machine\s+learning|ml)') then 'ml_engineer'
            when REGEXP_CONTAINS(intitule, r'(?i)data\s+scientist') then 'data_scientist'
            else 'autre_it'
        end as categorie_metier,
        -- Flag métier data
        (REGEXP_CONTAINS(intitule, r'(?i)data') 
         or REGEXP_CONTAINS(intitule, r'(?i)ml|scientist|engineer|analyst')) as is_metier_data
    from deduplicated
),

enhanced as (
    select *,
        -- 1. On calcule d'abord les valeurs de base
        case
            when code_commune is not null and length(cast(code_commune as string)) >= 2
                then substr(cast(code_commune as string), 1, 
                     case when cast(code_commune as string) >= '97' then 3 else 2 end)
            else regexp_extract(libelle_lieu, r'^(\d{2,3})')
        end as code_departement,

        case
            when experience_exige = 'D' then null
            when REGEXP_CONTAINS(experience_libelle, r'(\d+)\s+An\(s\)') then CAST(REGEXP_EXTRACT(experience_libelle, r'(\d+)\s+An\(s\)') AS FLOAT64)
            when REGEXP_CONTAINS(experience_libelle, r'(\d+)\s+Mois') then CAST(REGEXP_EXTRACT(experience_libelle, r'(\d+)\s+Mois') AS FLOAT64) / 12.0
            else null
        end as experience_duree_annees,

        case
            when salaire_periodicite = 'annuel' then salaire_min_euros
            when salaire_periodicite = 'mensuel' and salaire_min_euros > 10000 then salaire_min_euros
            when salaire_periodicite = 'mensuel' then salaire_min_euros * coalesce(salaire_nb_mois, 12)
            when salaire_periodicite = 'horaire' and salaire_min_euros > 100 then salaire_min_euros
            when salaire_periodicite = 'horaire' then salaire_min_euros * 1607
            else null
        end as salaire_annuel_min,

        case
            when salaire_periodicite = 'annuel' then coalesce(salaire_max_euros, salaire_min_euros)
            when salaire_periodicite = 'mensuel' and salaire_min_euros > 10000 then coalesce(salaire_max_euros, salaire_min_euros)
            when salaire_periodicite = 'mensuel' then coalesce(salaire_max_euros, salaire_min_euros) * coalesce(salaire_nb_mois, 12)
            when salaire_periodicite = 'horaire' and salaire_min_euros > 100 then coalesce(salaire_max_euros, salaire_min_euros)
            when salaire_periodicite = 'horaire' then coalesce(salaire_max_euros, salaire_min_euros) * 1607
            else null
        end as salaire_annuel_max
    from with_metier
),

final_flags as (
    select *,
        -- Maintenant salaire_annuel_min est disponible car on "vient de" enhanced
        (salaire_annuel_min < 15000 or salaire_annuel_min = 0) as is_salaire_aberrant,

        (salaire_periodicite = 'mensuel' and salaire_min_euros > 10000) 
        or (salaire_periodicite = 'horaire' and salaire_min_euros > 100) as is_salaire_requalifie,

        case 
            when code_naf in ('78.10Z', '78.20Z') then true
            when code_naf is null then null
            else false
        end as is_intermediaire
    from enhanced
)

select *
from final_flags