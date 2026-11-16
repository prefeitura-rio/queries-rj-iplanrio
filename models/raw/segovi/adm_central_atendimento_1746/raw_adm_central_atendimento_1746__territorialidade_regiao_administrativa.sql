{{
    config(
        alias='territorialidade_regiao_administrativa',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_regiao_administrativa AS STRING) AS ds_regiao_administrativa,
    SAFE_CAST(id_territorialidade_fk AS INT64) AS id_territorialidade_fk,
    SAFE_CAST(id_territorialidade_regiao_administrativa AS INT64) AS id_territorialidade_regiao_administrativa,
    SAFE_CAST(no_regiao_administrativa AS STRING) AS no_regiao_administrativa,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_territorialidade_regiao_administrativa') }}
