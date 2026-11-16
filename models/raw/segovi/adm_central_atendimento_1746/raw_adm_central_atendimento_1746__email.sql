{{
    config(
        alias='email',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_email AS STRING) AS ds_email,
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(id_email AS INT64) AS id_email,
    SAFE_CAST(id_setor_fk AS INT64) AS id_setor_fk,
    SAFE_CAST(id_unidade_organizacional_fk AS INT64) AS id_unidade_organizacional_fk,
    SAFE_CAST(no_pessoa AS STRING) AS no_pessoa,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_email') }}
