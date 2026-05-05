SELECT
    source,
    offre_id,
    COUNT(*) AS nb_lignes
FROM {{ ref('int_offres_enrichies') }}
GROUP BY source, offre_id
HAVING COUNT(*) > 1