{{
    config(
        schema='forca_municipal',
        alias="qmd_servicos",
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(Id AS INT64) AS id_servico,
    SAFE_CAST(IdPlano AS INT64) AS id_plano_servico,
    SAFE_CAST(IdQmd AS INT64) AS id_qmd,
    SAFE_CAST(Nome AS STRING) AS nome_servico,
    SAFE_CAST(Dias AS STRING) AS dias_semana_execucao
FROM {{ source('brutos_forca_municipal_staging', 'qmd_servicos') }}
