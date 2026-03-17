{{
    config(
        alias='averbacoes_contagem',
        materialized="table",
        tags=["raw", "ergon", "averbacoes", "contagem"],
        description="Tabela que armazena informações referentes sobre finalidade de contagem e total do tempo de contagem referentes a períodos averbados pelos servidores."
    )
}}

SELECT
    SAFE_CAST(NUMFUNC AS int64) AS id_funcionario,
    SAFE_CAST(NUMVINC AS int64) AS id_vinculo,
    SAFE_CAST(CHAVEAVERB AS int64) AS id_averbacao,
    SAFE_CAST(FINALIDADE AS STRING) AS finalidade,
    SAFE_CAST(DIAS AS int64) AS dias
FROM {{ source('brutos_ergon_staging', 'AVERB_OQUE_CONTA') }} AS t