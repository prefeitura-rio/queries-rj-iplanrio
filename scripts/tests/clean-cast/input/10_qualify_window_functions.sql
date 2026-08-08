-- Test Case 10: QUALIFY with window functions
-- Expected: Apply transformations on source data

WITH ranked AS (
    SELECT
        SAFE_CAST(REGEXP_REPLACE(id_func, r'\.0$', '') AS STRING) AS id_funcionario,
        SAFE_CAST(REGEXP_REPLACE(id_vinc, r'\.0$', '') AS INT64) AS id_vinculo,
        SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(nome AS STRING)), r'\.0$', '') AS STRING) AS nome,
        data_inicio,
        data_fim
    FROM {{ source('ergon', 'funcionarios') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_funcionario
        ORDER BY id_vinculo DESC, data_inicio DESC
    ) = 1
)

SELECT * FROM ranked
