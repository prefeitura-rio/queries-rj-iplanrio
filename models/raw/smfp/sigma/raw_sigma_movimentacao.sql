{{
    config(
        alias='movimentacao',
        description="Movimentações de estoque"
    )
}}

SELECT
    SAFE_CAST(CD_MATERIAL AS STRING) AS codigo_material,
    SAFE_CAST(CNPJ_FORNECEDOR AS STRING) AS cnpj_fornecedor,
    SAFE_CAST(NOTA_FISCAL AS NUMERIC) AS nota_fiscal,
    SAFE_CAST(SERIE AS STRING) AS serie_nota_fiscal,
    SAFE_CAST(DATA_NOTA_FISCAL AS NUMERIC) AS data_nota_fiscal,
    SAFE_CAST(QUANTIDADE_ITEM AS NUMERIC) AS quantidade_item,
    SAFE_CAST(PRECO_ITEM AS NUMERIC) AS preco_item,
    SAFE_CAST(TOTAL_ITEM AS NUMERIC) AS total_item,
    SAFE_CAST(DATA_ULTIMA_ATUALIZACAO AS NUMERIC) AS data_ultima_atualizacao,
    SAFE_CAST(CD_MOVIMENTACAO AS NUMERIC) AS codigo_movimentcao,
    SAFE_CAST(DS_MOVIMENTACAO AS STRING) AS tipo_movimentcao,
    SAFE_CAST(TP_ALMOXARIFADO AS STRING) AS tipo_almoxarifado,
    SAFE_CAST(CD_SECRETARIA AS NUMERIC) AS codigo_secretaria,
    SAFE_CAST(DS_SECRETARIA AS STRING) AS descricao_secretaria,
    SAFE_CAST(CD_ALMOXARIFADO_DESTINO AS NUMERIC) AS id_almoxarifado_destino,
    SAFE_CAST(DS_ALMOXARIFADO_DESTINO AS STRING) AS descricao_almoxarifado_destino,
    SAFE_CAST(CD_ALMOXARIFADO_ORIGEM AS NUMERIC) AS id_almoxarifado_origem,
    SAFE_CAST(DS_ALMOXARIFADO_ORIGEM AS STRING) AS descricao_almoxarifado_origem,
    SAFE_CAST(CD_OS AS STRING) AS codigo_organizacao_social,
    SAFE_CAST(DT_INI_CONTRATO_OS AS NUMERIC) AS data_inicio_contrato,
    SAFE_CAST(DT_FIM_CONTRATO_OS AS NUMERIC) AS data_fim_contrato,
    SAFE_CAST(NR_EMPENHO AS NUMERIC) AS codigo_empenho,
    SAFE_CAST(CNPJ_FABRICANTE AS NUMERIC) AS cnpj_fabricante
FROM {{ source('brutos_compras_materiais_servicos_sigma_staging', 'movimentacao') }}