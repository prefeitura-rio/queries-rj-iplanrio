{{
    config(
        alias='meta'
    )
}}

SELECT
    SAFE_CAST(metaID AS STRING)	id_meta,
    SAFE_CAST(descricao AS STRING) descricao,
    SAFE_CAST(observacao AS STRING) observacao,
    SAFE_CAST(ordemOrgao AS INT64) ordem,
    SAFE_CAST(valorReferencia AS FLOAT64) referencia,
    SAFE_CAST(dataReferencia AS DATE) data_referencia,
    SAFE_CAST(ano AS INT64) ano,
    SAFE_CAST(fonte AS STRING) fonte,
    SAFE_CAST(formula AS STRING) formula,
    SAFE_CAST(TipoAcompanhamento AS STRING) tipo_acompanhamento,
    SAFE_CAST(TipoGestao AS STRING) tipo_gestao,
    SAFE_CAST(orgaoID AS STRING) id_orgao,
    SAFE_CAST(orgao AS STRING) orgao,
    SAFE_CAST(SiglaOrgao AS STRING)	sigla_orgao,
    SAFE_CAST(unidade_de_medida AS STRING) unidade_medida,
    SAFE_CAST(metaMaeID AS STRING) numero_meta_mae,
    SAFE_CAST(AreaResultado AS STRING) area_resultado,
    SAFE_CAST(TipoMeta AS STRING) tipo,
    SAFE_CAST(recurso AS STRING) indicador_recurso,
    SAFE_CAST(auditoria AS STRING) indicador_auditoria,
    SAFE_CAST(janeiro AS FLOAT64) janeiro,
    SAFE_CAST(fevereiro AS FLOAT64)	fevereiro,
    SAFE_CAST(marco AS FLOAT64)	marco,
    SAFE_CAST(abril AS FLOAT64) abril,
    SAFE_CAST(maio AS FLOAT64) maio,
    SAFE_CAST(junho AS FLOAT64) junho,
    SAFE_CAST(julho AS FLOAT64) julho,
    SAFE_CAST(agosto AS FLOAT64) agosto,
    SAFE_CAST(setembro AS FLOAT64) setembro,
    SAFE_CAST(outubro AS FLOAT64) outubro,
    SAFE_CAST(novembro AS FLOAT64) novembro,
    SAFE_CAST(dezembro AS FLOAT64) dezembro
FROM {{ source('planejamento_gestao_dashboard_metas_staging', 'meta') }}
    WHERE painel = 'EGPRio'
