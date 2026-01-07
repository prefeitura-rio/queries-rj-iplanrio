{{
    config(
        alias='classificacao_atributo',
        schema='adm_central_atendimento_1746',
        materialized='view',
    )
}}

SELECT *
FROM {{ source('brutos_1746_staging_airbyte', 'tb_classificacao_atributo') }}

