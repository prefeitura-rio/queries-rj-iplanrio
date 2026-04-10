-- Test Case 11: ARRAY_AGG with STRUCT
-- Expected: Apply transformations on source, complex aggregation preserved

SELECT
    {{ clean_and_cast('cpf', 'string', trim=false) }} AS cpf,
    ARRAY_AGG(
        STRUCT(
            {{ clean_and_cast('id_func', 'string', trim=false) }} AS id_funcionario,
            {{ clean_and_cast('matricula', 'string', trim=false) }} AS matricula,
            {{ clean_and_cast('nome', 'string', trim=false) }} AS nome,
            data_inicio,
            data_fim
        )
        ORDER BY data_inicio DESC
    ) AS historico
FROM {{ source('ergon', 'funcionarios_historico') }}
GROUP BY cpf
