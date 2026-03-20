{{
    config(
        alias="orcamento_licitado",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT
    DISTINCT
        {{ clean_and_cast('cd_obra', 'string') }} id_obra,
        SAFE_CAST(ds_titulo_objeto AS STRING) titulo_objeto,
        SAFE_CAST(nm_sistema AS STRING) nome_sistema,
        SAFE_CAST(nm_sub_sistema AS STRING) nome_subsistema,
        SAFE_CAST(nm_planilha AS STRING) nome_planilha,
        {{ clean_and_cast('nr_item', 'string') }} numero_item,
        SAFE_CAST(cd_chave_externa AS STRING) chave_externa,
        SAFE_CAST(ds_item_servico AS STRING) descricao_item_servico,
        SAFE_CAST(tx_unidade_medida AS STRING) unidade_medida,
        SAFE_CAST(qt_contratado AS FLOAT64) quantidade_contratada,
        SAFE_CAST(vl_unitario AS FLOAT64) valor_unitario,
        SAFE_CAST(vl_total AS FLOAT64) valor_total
FROM {{ source('brutos_siscob_staging', 'orcamento_licitado') }} AS t