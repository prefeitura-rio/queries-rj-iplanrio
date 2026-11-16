{{
    config(
        alias='status',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_sms AS STRING) AS ds_sms,
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(fl_encerramento AS BOOL) AS fl_encerramento,
    SAFE_CAST(fl_falha_integracao AS BOOL) AS fl_falha_integracao,
    SAFE_CAST(fl_nao_conta_sla AS BOOL) AS fl_nao_conta_sla,
    SAFE_CAST(fl_solucao AS BOOL) AS fl_solucao,
    SAFE_CAST(id_status AS INT64) AS id_status,
    SAFE_CAST(no_status AS STRING) AS no_status,
    SAFE_CAST(no_status_espanhol AS STRING) AS no_status_espanhol,
    SAFE_CAST(no_status_ingles AS STRING) AS no_status_ingles,
    SAFE_CAST(no_status_view AS STRING) AS no_status_view,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_status') }}
