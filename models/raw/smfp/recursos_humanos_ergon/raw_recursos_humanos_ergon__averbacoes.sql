{{
    config(
        alias='averbacoes',
    )
}}

SELECT
    SAFE_CAST(NUMFUNC AS STRING) AS id_funcionario,
    SAFE_CAST(NUMVINC AS STRING) AS id_vinculo,
    SAFE_CAST(CHAVE AS STRING) AS id_averbacao,
    SAFE_CAST(DTINI AS STRING) AS data_inicio,
    SAFE_CAST(DTFIM AS STRING) AS data_final,
    SAFE_CAST(INSTITUICAO AS STRING) AS instituicao,
    SAFE_CAST(TIPOTEMPO AS STRING) AS id_tipo_tempo,
    SAFE_CAST(DATA_A_CONTAR AS STRING) AS data_validade,
    SAFE_CAST(TOTAL_DIAS AS STRING) AS total_dias_averbados,
    SAFE_CAST(MOTIVO AS STRING) AS motivo,
    SAFE_CAST(SOBREPOE AS STRING) AS sobrepoe,
    SAFE_CAST(EMP_CODIGO AS STRING) AS id_empresa,
    SAFE_CAST(OBS AS STRING) AS obs,
    SAFE_CAST(REGPREV AS STRING) AS regime_previdenciario,
FROM {{ source('brutos_ergon_staging', 'AVERBACOES_CONTA') }} AS t