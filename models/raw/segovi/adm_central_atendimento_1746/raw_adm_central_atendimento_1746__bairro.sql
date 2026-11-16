{{
    config(
        alias='bairro',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(id_bairro AS INT64) AS id_bairro,
    SAFE_CAST(no_bairro AS STRING) AS no_bairro,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_bairro') }}
