-- Test Case 9: Subquery in FROM clause
-- Expected: Apply transformation in source subquery, skip in ref subquery

SELECT
    src.id_source,
    src.nome_source,
    ref.id_ref,
    ref.nome_ref
FROM (
    SELECT
        SAFE_CAST(REGEXP_REPLACE(id_col, r'\.0$', '') AS STRING) AS id_source,
        SAFE_CAST(REGEXP_REPLACE(nome, r'\.0$', '') AS STRING) AS nome_source
    FROM {{ source('dataset', 'raw_table') }}
) src
LEFT JOIN (
    SELECT
        SAFE_CAST(REGEXP_REPLACE(id_col, r'\.0$', '') AS STRING) AS id_ref,
        SAFE_CAST(REGEXP_REPLACE(nome, r'\.0$', '') AS STRING) AS nome_ref
    FROM {{ ref('processed_table') }}
) ref ON src.id_source = ref.id_ref
