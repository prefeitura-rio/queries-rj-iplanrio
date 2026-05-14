{{
    config(
        schema='forca_municipal',
        alias="qmd",
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(Id AS INT64) AS id_qmd,
    SAFE_CAST(Nome AS STRING) AS nome_qmd,
    SAFE_CAST(Area AS STRING) AS area_qmd,
    SAFE_CAST(Resumo AS STRING) AS resumo_qmd,
    SAFE_CAST(Prescricoes AS STRING) AS prescricoes_qmd,
    SAFE_CAST(DataVigenciaInicio AS TIMESTAMP) AS data_vigencia_inicio,
    SAFE_CAST(DataVigenciaFim AS TIMESTAMP) AS data_vigencia_fim,
    SAFE_CAST(HoraExecucaoInicio AS STRING) AS hora_execucao_inicio,
    SAFE_CAST(HoraExecucaoFim AS STRING) AS hora_execucao_fim,
    SAFE_CAST(StatusAtivo AS BOOL) AS indicador_qmd_ativo,
    SAFE_CAST(StatusValido AS BOOL) AS indicador_qmd_valido,
    SAFE_CAST(StatusAutorizado AS BOOL) AS indicador_qmd_autorizado,
    SAFE_CAST(IdRespCriacao AS INT64) AS id_responsavel_criacao,
    SAFE_CAST(REPLACE(IdRespAutorizacao, ".0", "") AS INT64) AS id_responsavel_autorizacao,
    SAFE_CAST(DataHoraCriacao AS TIMESTAMP) AS data_hora_criacao,
    SAFE_CAST(DataHoraAutorizacao AS TIMESTAMP) AS data_hora_autorizacao
FROM {{ source('brutos_forca_municipal_staging', 'qmd') }}
