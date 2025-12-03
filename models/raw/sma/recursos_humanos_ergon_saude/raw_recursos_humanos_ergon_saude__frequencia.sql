
{{
    config(
        alias='frequencia_ergon',
    )
}}

SELECT
    SAFE_CAST(NUMFUNC AS STRING) AS id_funcionario,
    SAFE_CAST(NUMVINC AS STRING) AS id_vinculo,
    SAFE_CAST(DTINI AS STRING) AS data_inicio,
    SAFE_CAST(DTFIM AS STRING) AS data_final,
    SAFE_CAST(TIPOFREQ AS STRING) AS tipo_frequencia,
    SAFE_CAST(CODFREQ AS STRING) AS id_frequencia,
    SAFE_CAST(OBS AS STRING) AS observacoes,
    SAFE_CAST(EMP_CODIGO AS STRING) AS id_empresa,
FROM {{ source('brutos_ergon_saude_staging', 'FREQUENCIAS') }} AS t
