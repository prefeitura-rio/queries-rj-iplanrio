{{
    config(
        alias='erg_pm_resultpront',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "erg_pm_resultpront"],
        description="Resultados dos prontuários da perícia médica."
    )
}}

SELECT
    SAFE_CAST(resultpront AS STRING) AS id_resultado_prontuario,
    SAFE_CAST(chavepront AS STRING) AS id_prontuario,
    SAFE_CAST(decisao AS STRING) AS id_decisao,
    SAFE_CAST(numvinc AS STRING) AS id_vinculo,
    SAFE_CAST(dtini AS DATE) AS data_inicio,
    SAFE_CAST(dtfim AS DATE) AS data_final,
    SAFE_CAST(numdias AS INT64) AS dias,
    SAFE_CAST(tipofreq AS STRING) AS tipo_afastamento,
    SAFE_CAST(codfreq AS STRING) AS id_afastamento,
    SAFE_CAST(pontpubl AS INT64) AS pontuacao_publicacao,
    SAFE_CAST(dtconcl AS DATE) AS data_conclusao,
    SAFE_CAST(erroconcl AS STRING) AS erro_conclusao,
    SAFE_CAST(resultretif AS STRING) AS id_resultado_retificacao,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    SAFE_CAST(flex_campo_06 AS STRING) AS flex_campo_06,
    SAFE_CAST(flex_campo_07 AS STRING) AS flex_campo_07,
    SAFE_CAST(flex_campo_08 AS STRING) AS flex_campo_08,
    SAFE_CAST(flex_campo_09 AS STRING) AS flex_campo_09,
    SAFE_CAST(flex_campo_10 AS STRING) AS flex_campo_10,
    SAFE_CAST(justificativa AS STRING) AS justificativa,
    SAFE_CAST(prazo AS STRING) AS prazo,
    SAFE_CAST(nexo AS STRING) AS nexo,
    SAFE_CAST(juridico AS STRING) AS juridico,
    SAFE_CAST(motivopubl AS STRING) AS motivo_publicacao,
    SAFE_CAST(versaopubl AS INT64) AS versao_publicacao,
    SAFE_CAST(flex_campo_11 AS STRING) AS flex_campo_11,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'erg_pm_resultpront') }} AS t
