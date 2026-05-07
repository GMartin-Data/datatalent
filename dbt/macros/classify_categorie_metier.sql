-- Macro : classify_categorie_metier
-- --------------------------------------------------------------------
-- Finalité : classifie un titre d'offre d'emploi en catégorie métier
--            (data_engineer, data_analyst, data_architect,
--             bi_decisionnel, ml_engineer, data_scientist, autre_it).
--
-- Paramètre :
--   titre_column (string) — Nom de colonne SQL (ou expression SQL)
--                            à classifier. Inséré tel quel dans le SQL
--                            généré à la compilation.
--
-- Retour :
--   Expression SQL CASE retournant un STRING parmi les 7 valeurs
--   listées ci-dessus. Utilisable dans une clause SELECT.
--
-- Version : 1.0 (création initiale)
--
-- Références :
--   - D85 — Macro classify_categorie_metier (première macro applicative du projet)
--   - D80 / D81 — harmonisation staging Adzuna ↔ FT
--   - Audit Étape 3, drifts 2.6 et 3.4 (audit-drift-mart-offres-flow.md)
--     Patterns retirés : `dceo engineer`,
--     `data cent(er|re) engineering operations` — 4 offres Amazon DCEO
--     classées 100% faux positifs.
--   - Spec FT §3.3 — dbt-stg-france-travail-offres.md (source de vérité)

{% macro classify_categorie_metier(titre_column) %}
CASE
  WHEN REGEXP_CONTAINS({{ titre_column }}, r'(?i)data\s+engineer|ing[eé]nieur\s+data|d[eé]veloppeur\s+data|data\s+engineering|ing[eé]nieur\s+de\s+donn[ée]es|data\s+ing[eé]nieur')
    THEN 'data_engineer'
  WHEN REGEXP_CONTAINS({{ titre_column }}, r'(?i)architecte\s+data|data\s+architect|architecte\s+de\s+donn[ée]es')
    THEN 'data_architect'
  WHEN REGEXP_CONTAINS({{ titre_column }}, r'(?i)data\s+analyst|analyste\s+data|business\s+data\s+analyst|data\s+product\s+analyst|analyste\s+de\s+donn[ée]es')
    THEN 'data_analyst'
  WHEN REGEXP_CONTAINS({{ titre_column }}, r'(?i)(?:d[eé]veloppeur|analyste|consultant|ing[eé]nieur)\s+(?:bi|d[eé]cisionnel|business\s+intelligence)|(?:bi|d[eé]cisionnel|business\s+intelligence)\s+(?:d[eé]veloppeur|analyste|consultant|ing[eé]nieur)|power\s*bi|d[eé]veloppeur\s+bi|\bbi\b\s+(?:analyst|developer)')
    THEN 'bi_decisionnel'
  WHEN REGEXP_CONTAINS({{ titre_column }}, r'(?i)mlops|ml\s+engineer|machine\s+learning\s+engineer|ing[eé]nieur\s+(?:machine\s+learning|ml)')
    THEN 'ml_engineer'
  WHEN REGEXP_CONTAINS({{ titre_column }}, r'(?i)data\s+scientist')
    THEN 'data_scientist'
  ELSE 'autre_it'
END
{% endmacro %}
