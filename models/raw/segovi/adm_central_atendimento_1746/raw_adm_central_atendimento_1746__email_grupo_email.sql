{{
    config(
        alias='email_grupo_email',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(id_cadastro_email_fk AS INT64) AS id_cadastro_email_fk,
    SAFE_CAST(id_email_grupo_email AS INT64) AS id_email_grupo_email,
    SAFE_CAST(id_grupo_email_fk AS INT64) AS id_grupo_email_fk,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_email_grupo_email') }}
