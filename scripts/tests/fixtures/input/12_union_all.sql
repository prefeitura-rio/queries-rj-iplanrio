-- Test Case 12: UNION ALL combining source and ref
-- Expected: Transform source part, skip ref part

SELECT
    SAFE_CAST(REGEXP_REPLACE(id_col, r'\.0$', '') AS STRING) AS id_item,
    SAFE_CAST(REGEXP_REPLACE(nome, r'\.0$', '') AS STRING) AS nome,
    'source_a' AS origem
FROM {{ source('schema_a', 'table_a') }}

UNION ALL

SELECT
    SAFE_CAST(REGEXP_REPLACE(id_col, r'\.0$', '') AS STRING) AS id_item,
    SAFE_CAST(REGEXP_REPLACE(nome, r'\.0$', '') AS STRING) AS nome,
    'source_b' AS origem
FROM {{ source('schema_b', 'table_b') }}

UNION ALL

SELECT
    SAFE_CAST(REGEXP_REPLACE(id_col, r'\.0$', '') AS STRING) AS id_item,
    SAFE_CAST(REGEXP_REPLACE(nome, r'\.0$', '') AS STRING) AS nome,
    'ref_processed' AS origem
FROM {{ ref('processed_data') }}
