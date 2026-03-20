{{
    config(
        alias="itens_medidos_finalizados",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}

SELECT
    DISTINCT
        {{ clean_and_cast('cd_obra', 'string') }} id_obra,
        SAFE_CAST(nm_sistema AS STRING) nome_sistema,
        SAFE_CAST(nm_sub_sistema AS STRING) nome_subsistema,
        SAFE_CAST(nm_planilha AS STRING) nome_planilha,
        {{ clean_and_cast('nr_item', 'string') }} numero_item,
        SAFE_CAST(cd_chave_externa AS STRING) chave_externa,
        SAFE_CAST(ds_item_servico AS STRING) descricao_item_servico,
        SAFE_CAST(tx_unidade_medida AS STRING) unidade_medida,
        SAFE_CAST(qt_contratada AS FLOAT64) quantidade_contratada,
        SAFE_CAST(vl_unitario_licitacao AS FLOAT64) valor_unitario_licitacao,
        SAFE_CAST(qt_acumulada AS FLOAT64) quantidade_acumulada,
        SAFE_CAST(vl_acumulado_medido AS FLOAT64) valor_acumulado_medido,
        SAFE_CAST(
            SAFE.PARSE_DATE('%Y-%m-%d', dt_fim_obra) AS DATE
        ) AS data_fim_obra
FROM {{ source('brutos_siscob_staging', 'itens_medidos_finalizados') }} AS t