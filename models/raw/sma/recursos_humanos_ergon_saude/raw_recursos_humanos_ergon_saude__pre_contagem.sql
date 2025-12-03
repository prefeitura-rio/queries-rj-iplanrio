{{
    config(
        alias='pre_contagem',
    )
}}

SELECT
    SAFE_CAST(FINALIDADE AS STRING) AS finalidade,
    SAFE_CAST(NUMFUNC AS STRING) AS id_funcionario,
    SAFE_CAST(NUMVINC AS STRING) AS id_vinculo,
    SAFE_CAST(PERIODOS AS STRING) AS periodos,
    SAFE_CAST(OFFSET AS STRING) AS dias,
    SAFE_CAST(DTINI AS STRING) AS data_validade,
    SAFE_CAST(EMP_CODIGO AS STRING) AS id_empresa,
    SAFE_CAST(FLEX_CAMPO_01 AS STRING) AS observacoes,
FROM {{ source('brutos_ergon_saude_staging', 'PRE_CONTA') }} AS t