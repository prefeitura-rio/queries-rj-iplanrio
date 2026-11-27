{{
    config(
        alias='concursos',
        materialized="table",
        tags=["raw", "ergon", "concursos"],
        description="Concursos públicos cadastrados."
    )
}}

SELECT
    SAFE_CAST(chave AS STRING) AS id_concurso,
    SAFE_CAST(sigla AS STRING) AS sigla,
    SAFE_CAST(regimejur AS STRING) AS regime_juridico,
    SAFE_CAST(dtini AS DATE) AS data_inicio,
    SAFE_CAST(validade AS DATE) AS data_validade,
    SAFE_CAST(public_edital AS DATE) AS data_publicacao_edital,
    SAFE_CAST(resultado AS DATE) AS data_resultado,
    SAFE_CAST(objetivo AS STRING) AS objetivo,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    SAFE_CAST(emp_codigo AS STRING) AS id_empresa,
    SAFE_CAST(prorrogacao AS DATE) AS data_prorrogacao,
    SAFE_CAST(cancelamento AS DATE) AS data_cancelamento,
    SAFE_CAST(abrangencia AS STRING) AS abrangencia,
    SAFE_CAST(pontpubl AS INT64) AS pontuacao_publicacao,
    SAFE_CAST(dtini_inscricao AS DATE) AS data_inicio_inscricao,
    SAFE_CAST(dtfim_inscricao AS DATE) AS data_fim_inscricao,
    SAFE_CAST(setor AS STRING) AS setor,
    SAFE_CAST(tipo_concurso AS STRING) AS tipo_concurso,
    SAFE_CAST(tipovinc AS STRING) AS tipo_vinculo,
    SAFE_CAST(instituicao AS STRING) AS id_instituicao,
    SAFE_CAST(dias_prazo_posse AS INT64) AS dias_prazo_posse,
    SAFE_CAST(dias_prazo_prorrog_posse AS INT64) AS dias_prazo_prorrog_posse,
    SAFE_CAST(dias_prazo_exerc AS INT64) AS dias_prazo_exerc,
    SAFE_CAST(dias_prazo_prorrog_exerc AS INT64) AS dias_prazo_prorrog_exerc,
    SAFE_CAST(flex_campo_06 AS STRING) AS flex_campo_06,
    SAFE_CAST(flex_campo_07 AS STRING) AS flex_campo_07,
    SAFE_CAST(flex_campo_08 AS STRING) AS flex_campo_08,
    SAFE_CAST(flex_campo_09 AS STRING) AS flex_campo_09,
    SAFE_CAST(flex_campo_10 AS STRING) AS flex_campo_10,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'concursos') }} AS t
