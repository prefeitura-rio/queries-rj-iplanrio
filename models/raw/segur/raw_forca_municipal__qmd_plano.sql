{{
    config(
        schema='forca_municipal',
        alias="qmd_plano",
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(Id AS INT64) AS id_plano,
    SAFE_CAST(Nome AS STRING) AS nome_plano,
    SAFE_CAST(SemanaReferenciaInicio AS TIMESTAMP) AS data_inicio_semana_referencia,
    SAFE_CAST(SemanaReferenciaFim AS TIMESTAMP) AS data_fim_semana_referencia,
    SAFE_CAST(IdRespCriacao AS INT64) AS id_responsavel_criacao,
    SAFE_CAST(DataHoraCriacao AS TIMESTAMP) AS data_hora_criacao
FROM {{ source('brutos_forca_municipal_staging', 'qmd_plano') }}
