

{{
    config(

        alias='licenca_afastamento',
        materialized="table",
        tags=["raw", "ergon", "afastamento"],
        description="Tabela que armazena as ocorrências de afastastamentos do servidor municipal."
    )
}}

SELECT
    SAFE_CAST(NUMFUNC AS string) AS id_funcionario,
    SAFE_CAST(NUMVINC AS string) AS id_vinculo,
    safe_cast(SAFE_CAST(DTINI AS timestamp) as date) AS data_inicio,
    safe_cast(SAFE_CAST(DTFIM AS timestamp) as date) AS data_final,
    SAFE_CAST(TIPOFREQ AS STRING) AS tipo_afastamento,
    SAFE_CAST(CODFREQ AS string) AS id_afastamento,
    SAFE_CAST(MOTIVO AS STRING) AS motivo,
    safe_cast(SAFE_CAST(DTPREVFIM AS timestamp) as date) AS data_previsao_retorno,
    SAFE_CAST(FLEX_CAMPO_01 AS STRING) AS parecer,
    safe.parse_date('%d/%m/%Y', FLEX_CAMPO_02) AS data_atendimento,
    SAFE_CAST(EMP_CODIGO AS string) AS id_empresa,
    SAFE_CAST(FLEX_CAMPO_07 AS STRING) AS crm
FROM {{ source('brutos_ergon_staging', 'LIC_AFAST') }} AS t
