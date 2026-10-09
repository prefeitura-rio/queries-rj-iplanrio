-- Test Case 10: QUALIFY with window functions
-- Expected: Apply transformations on source data

WITH ranked AS (
    SELECT
        {{ clean_and_cast('id_func', 'string', trim=false) }} AS id_funcionario,
        {{ clean_and_cast('id_vinc', 'int64', trim=false) }} AS id_vinculo,
        {{ clean_and_cast('nome', 'string') }} AS nome,
        data_inicio,
        data_fim
    FROM {{ source('ergon', 'funcionarios') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_funcionario
        ORDER BY id_vinculo DESC, data_inicio DESC
    ) = 1
)

SELECT * FROM ranked
