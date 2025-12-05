{{
    config(
        alias='tpmrj_pm_resultjunta',
        
    )
}}

SELECT safe_cast(CRM AS STRING) AS crm, 
safe_cast(DTFIM AS DATE) AS dt_fim, 
safe_cast(DTINI AS DATE) AS dt_inicio, 
safe_cast(CODFREQ AS STRING) AS id_frequencia, 
safe_cast(DECISAO AS STRING) AS id_decisao, 
safe_cast(NUMDIAS AS INT64) AS num_dias, 
safe_cast(NUMVINC AS STRING) AS id_vinculo, 
safe_cast(ID_JUNTA AS STRING) AS id_junta, 
safe_cast(PONTPUBL AS STRING) AS ponto_publicado, 
safe_cast(TIPOFREQ AS STRING) AS tipo_frequencia, 
safe_cast(ERROCONCL AS STRING) AS erro_conclusao, 
safe_cast(LOTE_PUBL AS STRING) AS lote_publicado, 
safe_cast(RETIFICADO AS STRING) AS retificado, 
safe_cast(JUSTIF_RETIF AS STRING) AS justificativa_retificada, 
safe_cast(PROFISSIONAL AS STRING) AS profissional, 
safe_cast(ID_RESULTJUNTA AS STRING) AS id_resultado_junta, 
safe_cast(COMPL_DECISAO_PUBL AS STRING) AS compl_decisao_publicada, 
safe_cast(ID_RESULTJUNTARETIF AS STRING) AS id_resultado_junta_retificada FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'TPMRJ_PM_RESULTJUNTA') }}

