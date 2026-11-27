{{
    config(
        alias='tpmrj_pm_resultjunta',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "tpmrj_pm_resultjunta"],
        description="Resultados das juntas médicas."
    )
}}

SELECT
    SAFE_CAST(id_resultjunta AS STRING) AS id_resultado_junta,
    SAFE_CAST(id_junta AS STRING) AS id_junta,
    SAFE_CAST(decisao AS STRING) AS id_decisao,
    SAFE_CAST(numvinc AS STRING) AS id_vinculo,
    SAFE_CAST(dtini AS DATE) AS data_inicio,
    SAFE_CAST(dtfim AS DATE) AS data_final,
    SAFE_CAST(numdias AS INT64) AS dias,
    SAFE_CAST(id_resultjuntaretif AS STRING) AS id_resultado_junta_retificacao,
    SAFE_CAST(pontpubl AS INT64) AS pontuacao_publicacao,
    SAFE_CAST(justif_retif AS STRING) AS justificativa_retificacao,
    SAFE_CAST(retificado AS STRING) AS retificado,
    SAFE_CAST(tipofreq AS STRING) AS tipo_afastamento,
    SAFE_CAST(codfreq AS STRING) AS id_afastamento,
    SAFE_CAST(crm AS STRING) AS crm,
    SAFE_CAST(profissional AS STRING) AS id_profissional,
    SAFE_CAST(compl_decisao_publ AS STRING) AS complemento_decisao_publicacao,
    SAFE_CAST(erroconcl AS STRING) AS erro_conclusao,
    SAFE_CAST(lote_publ AS STRING) AS lote_publicacao,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'tpmrj_pm_resultjunta') }} AS t
