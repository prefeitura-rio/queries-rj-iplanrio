{{
    config(
        alias='envio_email_destinatario',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(data_resposta AS DATETIME) AS data_resposta,
    SAFE_CAST(ds_resposta AS STRING) AS ds_resposta,
    SAFE_CAST(fl_lido AS BOOL) AS fl_lido,
    SAFE_CAST(ic_tipo_destinatario AS STRING) AS ic_tipo_destinatario,
    SAFE_CAST(id_email_fk AS INT64) AS id_email_fk,
    SAFE_CAST(id_envio_email_destinatario AS INT64) AS id_envio_email_destinatario,
    SAFE_CAST(id_envio_email_fk AS INT64) AS id_envio_email_fk,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_envio_email_destinatario') }}
