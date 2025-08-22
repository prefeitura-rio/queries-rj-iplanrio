{{
    config(
        alias='movimentacao',
        schema='compras_materiais_servicos_sigma'
    )
}}

SELECT
    SAFE_CAST(cd_almoxarifado_destino AS STRING) AS id_almoxarifado_destino,
    SAFE_CAST(cd_almoxarifado_origem AS STRING) AS id_almoxarifado_origem,
    SAFE_CAST(cd_material AS STRING) AS codigo_material,
    SAFE_CAST(cd_movimentacao AS STRING) AS codigo_movimentcao,
    SAFE_CAST(cd_os AS STRING) AS codigo_organizacao_social,
    SAFE_CAST(cd_secretaria AS STRING) AS codigo_secretaria,
    SAFE_CAST(cnpj_fabricante AS STRING) AS cnpj_fabricante,
    SAFE_CAST(cnpj_fornecedor AS STRING) AS cnpj_fornecedor,
    SAFE_CAST(data_nota_fiscal AS STRING) AS data_nota_fiscal,
    SAFE_CAST(data_ultima_atualizacao AS STRING) AS data_ultima_atualizacao,
    SAFE_CAST(ds_almoxarifado_destino AS STRING) AS descricao_almoxarifado_destino,
    SAFE_CAST(ds_almoxarifado_origem AS STRING) AS descricao_almoxarifado_origem,
    SAFE_CAST(ds_movimentacao AS STRING) AS tipo_movimentcao,
    SAFE_CAST(ds_secretaria AS STRING) AS descricao_secretaria,
    SAFE_CAST(dt_fim_contrato_os AS STRING) AS data_fim_contrato,
    SAFE_CAST(dt_ini_contrato_os AS STRING) AS data_inicio_contrato,
    SAFE_CAST(nota_fiscal AS STRING) AS nota_fiscal,
    SAFE_CAST(nr_empenho AS STRING) AS codigo_empenho,
    SAFE_CAST(preco_item AS FLOAT64) AS preco_item,
    SAFE_CAST(quantidade_item AS INT64) AS quantidade_item,
    SAFE_CAST(serie AS STRING) AS serie_nota_fiscal,
    SAFE_CAST(total_item AS INT64) AS total_item,
    SAFE_CAST(tp_almoxarifado AS STRING) AS tipo_almoxarifado

from {{ source('compras_materiais_servicos_sigma_staging', 'VW_MOVIMENTACAO')}}