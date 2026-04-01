-- TODO : voir dbt-stg-sirene-etablissements.md
-- Matérialisé en table (pas view) : 43M lignes raw, filtre à 17M.
{{ config(materialized='table') }}