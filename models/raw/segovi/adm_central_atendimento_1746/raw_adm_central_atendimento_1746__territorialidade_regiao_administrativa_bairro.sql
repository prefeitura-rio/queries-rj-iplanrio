{{
    config(
        alias='territorialidade_regiao_administrativa_bairro',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(id_bairro_fk AS INT64) AS id_bairro_fk,
    SAFE_CAST(id_territorialidade_regiao_administrativa_bairro AS INT64) AS id_territorialidade_regiao_administrativa_bairro,
    SAFE_CAST(id_territorialidade_regiao_administrativa_fk AS INT64) AS id_territorialidade_regiao_administrativa_fk,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_territorialidade_regiao_administrativa_bairro') }}
