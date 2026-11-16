{{
    config(
        alias='tipo',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(id_tipo AS INT64) AS id_tipo,
    SAFE_CAST(no_tipo AS STRING) AS no_tipo,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_tipo') }}
