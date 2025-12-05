{{
    config(
        alias='tpmrj_pm_vwtpconclper',
       
    )
}}

SELECT safe_cast(CHAVE AS STRING) AS chave, 
    safe_cast(DECISAO AS STRING) AS decisao, 
    safe_cast(DTATEND AS DATE) AS dt_atendimento, 
    safe_cast(MATRICULA AS STRING) AS matricula,
    safe_cast(NOMEGRUPO AS STRING) AS nome_grupo,
    safe_cast(GRUPOEXAME AS STRING) AS grupo_exame,
    safe_cast(SIGLAEXAME AS STRING) AS sigla_exame,
    safe_cast(DTCONCLUSAO AS DATE) AS dt_conclusao,
    safe_cast(SIGLAEXAME2 AS STRING) AS sigla_exame2,
    safe_cast(NOMESERVIDOR AS STRING) AS nome_servidor,
    safe_cast(SIGLA_DECISAO AS STRING) AS sigla_decisao,
    safe_cast(SIGLA_DECISAO2 AS STRING) AS sigla_decisao2 
FROM{{ source('brutos_recursos_humanos_ergon_pericia_medica_staging', 'TPMRJ_PM_VWTPCONCLPER') }}

