{{
    config(
        alias='erg_pm_resultpront'
    )
}}

SELECT safe_cast(NEXO AS STRING) AS nexo,
    safe_cast(DTFIM AS DATE) AS data_fim,
    safe_cast(DTINI AS DATE) AS data_inicio,
    safe_cast(PRAZO AS INT64) AS prazo,
    safe_cast(CODFREQ AS STRING) AS id_frequencia,
    safe_cast(DECISAO AS STRING) AS id_decisao,
    safe_cast(DTCONCL AS DATE) AS data_conclusao,
    safe_cast(NUMDIAS AS INT64) AS num_dias,
    safe_cast(NUMVINC AS STRING) AS id_vinculo,
    safe_cast(JURIDICO AS STRING) AS juridico,
    safe_cast(PONTPUBL AS STRING) AS ponto_publicacao,
    safe_cast(TIPOFREQ AS STRING) AS tipo_frequencia,
    safe_cast(ERROCONCL AS STRING) AS erro_conclusao,
    safe_cast(CHAVEPRONT AS STRING) AS chave_prontuario,
    safe_cast(MOTIVOPUBL AS STRING) AS motivo_publicacao,
    safe_cast(VERSAOPUBL AS STRING) AS versao_publicacao,
    safe_cast(RESULTPRONT AS STRING) AS resultado_prontuario,
    safe_cast(RESULTRETIF AS STRING) AS resultado_retificado,
    safe_cast(FLEX_CAMPO_01 AS STRING) AS flex_campo_01,
    safe_cast(FLEX_CAMPO_02 AS STRING) AS flex_campo_02,
    safe_cast(FLEX_CAMPO_03 AS STRING) AS flex_campo_03,
    safe_cast(FLEX_CAMPO_04 AS STRING) AS flex_campo_04,
    safe_cast(FLEX_CAMPO_05 AS STRING) AS flex_campo_05,
    safe_cast(FLEX_CAMPO_06 AS STRING) AS flex_campo_06,
    safe_cast(FLEX_CAMPO_07 AS STRING) AS flex_campo_07,
    safe_cast(FLEX_CAMPO_08 AS STRING) AS flex_campo_08,
    safe_cast(FLEX_CAMPO_09 AS STRING) AS flex_campo_09,
    safe_cast(FLEX_CAMPO_10 AS STRING) AS flex_campo_10,
    safe_cast(FLEX_CAMPO_11 AS STRING) AS flex_campo_11,
    safe_cast(JUSTIFICATIVA AS STRING) AS justificativa
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'ERG_PM_RESULTPRONT') }}
