-- Test Case 5: id_* columns from source
-- Expected: ALL id_* should be converted to STRING

SELECT
    SAFE_CAST(NUMFUNC AS int64) AS id_funcionario,
    SAFE_CAST(NUMVINC AS int64) AS id_vinculo,
    SAFE_CAST(REGEXP_REPLACE(EMP_CODIGO, r'\.0$', '') AS int64) AS id_empresa,
    {{ clean_and_cast('CHAVE', 'int64') }} AS id_averbacao,
    nome,
    cpf
FROM {{ source('ergon', 'funcionarios') }}
