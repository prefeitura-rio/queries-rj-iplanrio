{{
    config(
        alias='classificacao',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT *
FROM {{ source('brutos_1746_staging_airbyte', 'tb_classificacao') }}

