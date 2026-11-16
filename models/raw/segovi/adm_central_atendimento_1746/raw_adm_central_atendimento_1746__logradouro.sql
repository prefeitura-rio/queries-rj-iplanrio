{{
    config(
        alias='logradouro',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(fl_ativo AS BOOL) AS fl_ativo,
    SAFE_CAST(id_logradouro AS INT64) AS id_logradouro,
    SAFE_CAST(no_logradouro AS STRING) AS no_logradouro,
    SAFE_CAST(no_logradouro_invertido AS STRING) AS no_logradouro_invertido,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_logradouro') }}
