{{
    config(
        alias='averbacoes_contagem',
    )
}}

SELECT
    SAFE_CAST(NUMFUNC AS STRING) AS id_funcionario,
    SAFE_CAST(NUMVINC AS STRING) AS id_vinculo,
    SAFE_CAST(CHAVEAVERB AS STRING) AS id_averbacao,
    SAFE_CAST(FINALIDADE AS STRING) AS finalidade,
    SAFE_CAST(DIAS AS STRING) AS dias,
FROM {{ source('brutos_ergon_staging', 'AVERB_OQUE_CONTA') }} AS t