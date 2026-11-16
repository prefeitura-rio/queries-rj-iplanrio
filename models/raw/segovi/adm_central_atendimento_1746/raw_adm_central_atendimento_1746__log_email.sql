{{
    config(
        alias='log_email',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_destinatario AS STRING) AS ds_destinatario,
    SAFE_CAST(ds_enviado_assunto AS STRING) AS ds_enviado_assunto,
    SAFE_CAST(ds_enviado_corpo AS STRING) AS ds_enviado_corpo,
    SAFE_CAST(ds_erro AS STRING) AS ds_erro,
    SAFE_CAST(dt_envio AS DATETIME) AS dt_envio,
    SAFE_CAST(dt_primeiro_envio AS DATETIME) AS dt_primeiro_envio,
    SAFE_CAST(id_chamado_fk AS INT64) AS id_chamado_fk,
    SAFE_CAST(id_envio_email_destinatario_fk AS INT64) AS id_envio_email_destinatario_fk,
    SAFE_CAST(id_log_email AS INT64) AS id_log_email,
    SAFE_CAST(id_log_webservice_fk AS INT64) AS id_log_webservice_fk,
    SAFE_CAST(id_protocolo_fk AS INT64) AS id_protocolo_fk,
    SAFE_CAST(id_reintegracao_fk AS INT64) AS id_reintegracao_fk,
    SAFE_CAST(id_template_email_fk AS INT64) AS id_template_email_fk,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_log_email') }}
