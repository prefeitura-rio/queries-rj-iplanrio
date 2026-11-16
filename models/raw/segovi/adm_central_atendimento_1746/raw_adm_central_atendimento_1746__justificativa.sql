{{
    config(
        alias='justificativa',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_sms AS STRING) AS ds_sms,
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(fl_falha_integracao AS BOOL) AS fl_falha_integracao,
    SAFE_CAST(id_justificativa AS INT64) AS id_justificativa,
    SAFE_CAST(no_justificativa AS STRING) AS no_justificativa,
    SAFE_CAST(no_justificativa_espanhol AS STRING) AS no_justificativa_espanhol,
    SAFE_CAST(no_justificativa_ingles AS STRING) AS no_justificativa_ingles,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_justificativa') }}
