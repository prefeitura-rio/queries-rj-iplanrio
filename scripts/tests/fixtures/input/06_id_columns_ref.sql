-- Test Case 6: id_* columns from ref
-- Expected: Should SKIP (already processed as string in raw layer)

SELECT
    SAFE_CAST(id_funcionario AS STRING) AS id_funcionario,
    {{ clean_and_cast('id_vinculo', 'int64') }} AS id_vinculo,
    nome,
    cpf
FROM {{ ref('raw_funcionarios') }}
