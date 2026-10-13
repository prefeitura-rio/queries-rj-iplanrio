-- Test Case 5: id_* columns from source
-- Expected: ALL id_* should be converted to STRING

SELECT
    {{ clean_and_cast('NUMFUNC', 'string') }} AS id_funcionario,
    {{ clean_and_cast('NUMVINC', 'string') }} AS id_vinculo,
    {{ clean_and_cast('EMP_CODIGO', 'string') }} AS id_empresa,
    {{ clean_and_cast('CHAVE', 'string') }} AS id_averbacao,
    nome,
    cpf
FROM {{ source('ergon', 'funcionarios') }}
