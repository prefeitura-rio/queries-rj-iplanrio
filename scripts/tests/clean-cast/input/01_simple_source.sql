-- Test Case 1: Simple SELECT from source
-- Expected: ALL transformations should be applied

SELECT
    SAFE_CAST(REGEXP_REPLACE(id_col, r'\.0$', '') AS STRING) AS id_col,
    SAFE_CAST(REGEXP_REPLACE(nome, r'\.0$', '') AS STRING) AS nome,
    SAFE_CAST(REGEXP_REPLACE(valor, r'\.0$', '') AS INT64) AS valor,
    SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(data AS STRING)), r'\.0$', '') AS DATE) AS data
FROM {{ source('schema', 'table') }}
