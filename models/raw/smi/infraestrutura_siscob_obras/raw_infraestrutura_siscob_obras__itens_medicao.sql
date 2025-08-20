{{
    config(
        alias="itens_medicao",
        schema='infraestrutura_siscob_obras',
        materialized="table",
    )
}}


SELECT
    DISTINCT
        SAFE_CAST(REGEXP_REPLACE(cd_obra, r'\.0$', '') AS STRING) id_obra,
        SAFE_CAST(ds_titulo_objeto AS STRING) titulo_objeto,
        SAFE_CAST(ds_estado AS STRING) estado,
        SAFE_CAST(REGEXP_REPLACE(nr_medicao, r'\.0$', '') AS STRING) id_medicao,
        SAFE_CAST(
            SAFE.PARSE_DATE('%Y-%m-%d', dt_ini_medicao) AS DATE
        ) AS data_inicio_medicao,
        SAFE_CAST(
            SAFE.PARSE_DATE('%Y-%m-%d', dt_fim_medicao) AS DATE
        ) AS data_fim_medicao,
        SAFE_CAST(REGEXP_REPLACE(cd_etapa, r'\.0$', '') AS STRING) id_etapa,
        SAFE_CAST(nm_sistema AS STRING) nome_sistema,
        SAFE_CAST(nm_sub_sistema AS STRING) nome_subsistema,
        SAFE_CAST(nm_planilha AS STRING) nome_planilha,
        SAFE_CAST(REGEXP_REPLACE(nr_item, r'\.0$', '') AS STRING) numero_item,
        SAFE_CAST(cd_chave_externa AS STRING) chave_externa,
        SAFE_CAST(ds_item_servico AS STRING) descricao_item_servico,
        SAFE_CAST(tx_unidade_medida AS STRING) unidade_medida,
        SAFE_CAST(vl_item_servico AS FLOAT64) valor_item_servico,
        SAFE_CAST(qt_medida AS FLOAT64) quantidade_medida,
        SAFE_CAST(qt_acumulada AS FLOAT64) quantidade_acumulada,
        SAFE_CAST(vl_medido AS FLOAT64) valor_medido
FROM {{ source('brutos_siscob_staging', 'itens_medicao') }} AS t
