{{
    config(
        alias='envio_email',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_assunto AS STRING) AS ds_assunto,
    SAFE_CAST(ds_corpo AS STRING) AS ds_corpo,
    SAFE_CAST(dt_envio AS DATETIME) AS dt_envio,
    SAFE_CAST(fl_dados_pessoais AS BOOL) AS fl_dados_pessoais,
    SAFE_CAST(id_chamado_fk AS INT64) AS id_chamado_fk,
    SAFE_CAST(id_envio_email AS INT64) AS id_envio_email,
    SAFE_CAST(id_pessoa_fk AS INT64) AS id_pessoa_fk,
    SAFE_CAST(id_unidade_organizacional_fk AS INT64) AS id_unidade_organizacional_fk,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_envio_email') }}
