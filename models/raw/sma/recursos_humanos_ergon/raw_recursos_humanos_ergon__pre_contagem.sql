{{
    config(
        alias='pre_contagem',
    )
}}

SELECT
    SAFE_CAST(FINALIDADE AS STRING) AS finalidade,
    SAFE_CAST(NUMFUNC AS int64) AS id_funcionario,
    SAFE_CAST(NUMVINC AS int64) AS id_vinculo,
    SAFE_CAST(PERIODOS AS int64) AS periodos,
    SAFE_CAST(OFFSET AS int64) AS dias,
    safe_cast(SAFE_CAST(DTINI AS timestamp) as date) AS data_validade,
    SAFE_CAST(EMP_CODIGO AS int64) AS id_empresa,
    SAFE_CAST(FLEX_CAMPO_01 AS STRING) AS observacoes
FROM {{ source('brutos_ergon_staging', 'PRE_CONTA') }} AS t