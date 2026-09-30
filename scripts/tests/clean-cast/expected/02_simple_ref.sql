-- Test Case 2: Simple SELECT from ref
-- Expected: NO transformations (already processed)

SELECT
    SAFE_CAST(REGEXP_REPLACE(id_col, r'\.0$', '') AS STRING) AS id_col,
    SAFE_CAST(REGEXP_REPLACE(nome, r'\.0$', '') AS STRING) AS nome,
    valor,
    data
FROM {{ ref('raw_model') }}
