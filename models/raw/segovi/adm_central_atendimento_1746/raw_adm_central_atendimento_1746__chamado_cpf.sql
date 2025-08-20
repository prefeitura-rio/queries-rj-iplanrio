{{
    config(
        alias='chamado_cpf',
        schema='adm_central_atendimento_1746',
        materialized='table',
        unique_key='id_chamado',
    )
}}

SELECT
    SAFE_CAST(
        REGEXP_REPLACE(id_chamado, r'\.0$', '') AS STRING
    ) id_chamado,
    cpf
FROM {{ source('brutos_1746_staging', 'chamado_cpf') }}