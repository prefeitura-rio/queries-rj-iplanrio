

{{
    config(

        alias='licenca_afastamento',
    )
}}
SELECT
    SAFE_CAST(NUMFUNC AS STRING) AS id_funcionario,
    SAFE_CAST(NUMVINC AS STRING) AS id_vinculo,
    SAFE_CAST(DTINI AS STRING) AS data_inicio,
    SAFE_CAST(DTFIM AS STRING) AS data_final,
    SAFE_CAST(TIPOFREQ AS STRING) AS tipo_afastamento,
    SAFE_CAST(CODFREQ AS STRING) AS id_afastamento,
    SAFE_CAST(MOTIVO AS STRING) AS motivo,
    SAFE_CAST(DTPREVFIM AS STRING) AS data_previsao_retorno,
    SAFE_CAST(FLEX_CAMPO_01 AS STRING) AS parecer,
    SAFE_CAST(FLEX_CAMPO_02 AS STRING) AS data_atendimento,
    SAFE_CAST(EMP_CODIGO AS STRING) AS id_empresa,
    SAFE_CAST(FLEX_CAMPO_07 AS STRING) AS crm
FROM {{ source('brutos_ergon_saude_staging', 'LIC_AFAST') }} AS t
