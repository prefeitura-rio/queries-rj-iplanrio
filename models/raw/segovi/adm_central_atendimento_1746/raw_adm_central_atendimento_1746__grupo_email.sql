{{
    config(
        alias='grupo_email',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_grupo_email AS STRING) AS ds_grupo_email,
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(id_grupo_email AS INT64) AS id_grupo_email,
    SAFE_CAST(id_unidade_organizacional_fk AS INT64) AS id_unidade_organizacional_fk,
    SAFE_CAST(nm_grupo_email AS STRING) AS nm_grupo_email,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_grupo_email') }}
