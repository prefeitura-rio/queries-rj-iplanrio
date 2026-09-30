-- Test Case 12: UNION ALL combining source and ref
-- Expected: Transform source part, skip ref part

SELECT
    {{ clean_and_cast('id_col', 'string', trim=false) }} AS id_item,
    {{ clean_and_cast('nome', 'string', trim=false) }} AS nome,
    'source_a' AS origem
FROM {{ source('schema_a', 'table_a') }}

UNION ALL

SELECT
    {{ clean_and_cast('id_col', 'string', trim=false) }} AS id_item,
    {{ clean_and_cast('nome', 'string', trim=false) }} AS nome,
    'source_b' AS origem
FROM {{ source('schema_b', 'table_b') }}

UNION ALL

SELECT
    SAFE_CAST(REGEXP_REPLACE(id_col, r'\.0$', '') AS STRING) AS id_item,
    SAFE_CAST(REGEXP_REPLACE(nome, r'\.0$', '') AS STRING) AS nome,
    'ref_processed' AS origem
FROM {{ ref('processed_data') }}
