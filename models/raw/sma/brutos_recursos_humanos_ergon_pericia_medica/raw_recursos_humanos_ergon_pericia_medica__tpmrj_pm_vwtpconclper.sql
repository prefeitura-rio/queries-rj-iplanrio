{{
    config(
        alias='tpmrj_pm_vwtpconclper',
        materialized="table",
        tags=["raw", "ergon", "pericia_medica", "tpmrj_pm_vwtpconclper"],
        description="Visão dos tipos de conclusão de perícia."
    )
}}

SELECT
    SAFE_CAST(grupoexame AS STRING) AS id_grupo_exame,
    SAFE_CAST(nomegrupo AS STRING) AS nome_grupo,
    SAFE_CAST(siglaexame AS STRING) AS sigla_exame,
    SAFE_CAST(siglaexame2 AS STRING) AS sigla_exame_2,
    SAFE_CAST(chave AS STRING) AS id_chave,
    SAFE_CAST(decisao AS STRING) AS id_decisao,
    SAFE_CAST(sigla_decisao AS STRING) AS sigla_decisao,
    SAFE_CAST(sigla_decisao2 AS STRING) AS sigla_decisao_2,
    SAFE_CAST(dtatend AS DATE) AS data_atendimento,
    SAFE_CAST(matricula AS STRING) AS matricula,
    SAFE_CAST(nomeservidor AS STRING) AS nome_servidor,
    SAFE_CAST(dtconclusao AS DATE) AS data_conclusao,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon_pericia_medica', 'tpmrj_pm_vwtpconclper') }} AS t
