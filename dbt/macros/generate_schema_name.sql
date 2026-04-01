-- Override du comportement par défaut de dbt.
-- Sans cette macro, dbt concatène le dataset du profil avec +schema :
--   dataset: staging + schema: intermediate → "staging_intermediate"
-- Avec cette macro, +schema est utilisé tel quel :
--   schema: intermediate → "intermediate"
-- Voir : https://docs.getdbt.com/docs/build/custom-schemas

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}