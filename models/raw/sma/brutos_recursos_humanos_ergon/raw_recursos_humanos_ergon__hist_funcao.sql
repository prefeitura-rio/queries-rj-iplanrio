{{
    config(
        alias='hist_funcao',
        materialized="table",
        tags=["raw", "ergon", "historico", "funcao"],
        description="Histórico de funções dos servidores."
    )
}}

SELECT
    SAFE_CAST(funcao AS STRING) AS id_funcao,
    SAFE_CAST(dtini AS DATE) AS data_inicio,
    SAFE_CAST(dtfim AS DATE) AS data_final,
    SAFE_CAST(tabvenc AS STRING) AS tabela_vencimentos,
    SAFE_CAST(referencia AS STRING) AS referencia,
    SAFE_CAST(natureza AS STRING) AS natureza,
    SAFE_CAST(obs AS STRING) AS observacoes,
    SAFE_CAST(pontlei AS INT64) AS pontuacao_lei,
    SAFE_CAST(flex_campo_01 AS STRING) AS flex_campo_01,
    SAFE_CAST(flex_campo_02 AS STRING) AS flex_campo_02,
    SAFE_CAST(flex_campo_03 AS STRING) AS flex_campo_03,
    SAFE_CAST(flex_campo_04 AS STRING) AS flex_campo_04,
    SAFE_CAST(flex_campo_05 AS STRING) AS flex_campo_05,
    _prefect_extracted_at AS updated_at
FROM {{ source('brutos_recursos_humanos_ergon', 'hist_funcao') }} AS t
