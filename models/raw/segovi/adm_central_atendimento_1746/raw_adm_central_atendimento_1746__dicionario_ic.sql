{{
    config(
        alias='dicionario_ic',
        schema='adm_central_atendimento_1746',
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(ds_coluna AS STRING) AS ds_coluna,
    SAFE_CAST(ds_ic AS STRING) AS ds_ic,
    SAFE_CAST(ds_tabela AS STRING) AS ds_tabela,
    SAFE_CAST(ic AS STRING) AS ic,
    SAFE_CAST(id_dicionario_ic AS INT64) AS id_dicionario_ic,
FROM {{ source('brutos_1746_staging_airbyte', 'tb_dicionario_ic') }}
