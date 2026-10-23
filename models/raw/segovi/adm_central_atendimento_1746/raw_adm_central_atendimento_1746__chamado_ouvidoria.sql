{{
    config(
        alias='chamado_ouvidoria',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT *
FROM {{ ref('raw_sgrc_1746__viewdatalake_ouvidoria') }}