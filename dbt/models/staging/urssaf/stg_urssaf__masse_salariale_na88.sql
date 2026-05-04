-- TODO : voir dbt-stg-urssaf-masse-salariale.md
with source as (
    select * from {{ source('urssaf', 'urssaf_masse_salariale') }}
),
renamed as (
    select
        code_na88,
        libelle_na88,
        annee as annee_date,
        _ingestion_date as ingested_at,
        masse_salariale_brute,
        effectifs_salaries_moyens,
        nb_etablissements
    from source
)
select * from renamed