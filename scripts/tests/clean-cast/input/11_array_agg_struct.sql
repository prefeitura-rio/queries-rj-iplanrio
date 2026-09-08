-- Test Case 11: ARRAY_AGG with STRUCT
-- Expected: Apply transformations on source, complex aggregation preserved

SELECT
    SAFE_CAST(REGEXP_REPLACE(cpf, r'\.0$', '') AS STRING) AS cpf,
    ARRAY_AGG(
        STRUCT(
            SAFE_CAST(REGEXP_REPLACE(id_func, r'\.0$', '') AS STRING) AS id_funcionario,
            SAFE_CAST(REGEXP_REPLACE(matricula, r'\.0$', '') AS STRING) AS matricula,
            SAFE_CAST(REGEXP_REPLACE(nome, r'\.0$', '') AS STRING) AS nome,
            data_inicio,
            data_fim
        )
        ORDER BY data_inicio DESC
    ) AS historico
FROM {{ source('ergon', 'funcionarios_historico') }}
GROUP BY cpf
