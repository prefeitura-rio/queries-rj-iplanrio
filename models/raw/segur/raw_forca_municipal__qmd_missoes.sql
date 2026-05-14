{{
    config(
        schema='forca_municipal',
        alias="qmd_missoes",
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(Id AS INT64) AS id_missao,
    SAFE_CAST(IdQmd AS INT64) AS id_qmd,
    SAFE_CAST(TipoMissao AS STRING) AS tipo_missao,
    SAFE_CAST(Roteiro AS STRING) AS roteiro_missao,
    SAFE_CAST(HoraInicio AS STRING) AS hora_inicio_missao,
    SAFE_CAST(HoraFim AS STRING) AS hora_fim_missao
FROM {{ source('brutos_forca_municipal_staging', 'qmd_missoes') }}
