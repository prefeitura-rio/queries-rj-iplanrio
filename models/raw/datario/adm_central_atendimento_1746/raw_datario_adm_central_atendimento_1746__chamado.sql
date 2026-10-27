{{
    config(
        alias='chamado',
        schema='adm_central_atendimento_1746',
        materialized='table',
        unique_key='id_chamado',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}

SELECT 
    * except(descricao)
FROM {{ ref("raw_adm_central_atendimento_1746__chamado") }}