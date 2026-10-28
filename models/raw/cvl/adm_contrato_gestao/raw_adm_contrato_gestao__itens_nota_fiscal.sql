{{
    config(
        alias='itens_nota_fiscal',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}


SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_item_nf AS STRING)), r'\.0$', '') AS STRING) AS id_item_nf,
  SAFE_CAST(TRIM(SAFE_CAST(cod_item_nf AS STRING)) AS STRING) AS cod_item_nf,
  SAFE_CAST(TRIM(SAFE_CAST(qtd_material AS STRING)) AS NUMERIC) AS qtd_material,
  SAFE_CAST(TRIM(SAFE_CAST(valor_unitario AS STRING)) AS NUMERIC) AS valor_unitario,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(referencia_mes_nf AS STRING)), r'\.0$', '') AS INT64) AS referencia_mes_nf,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(referencia_ano_nf AS STRING)), r'\.0$', '') AS INT64) AS referencia_ano_nf,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_fornecedor AS STRING)), r'\.0$', '') AS STRING) AS id_fornecedor,
  SAFE_CAST(TRIM(SAFE_CAST(valor_total AS STRING)) AS NUMERIC) AS valor_total,
  SAFE_CAST(TRIM(SAFE_CAST(num_documento AS STRING)) AS STRING) AS num_documento,
  SAFE_CAST(TRIM(SAFE_CAST(cod_organizacao AS STRING)) AS STRING) AS cod_instituicao,
  SAFE_CAST(data_envio AS DATETIME) AS data_envio,
  SAFE_CAST(TRIM(SAFE_CAST(tipo_item AS STRING)) AS STRING) AS tipo_item,
  SAFE_CAST(TRIM(SAFE_CAST(item AS STRING)) AS STRING) AS item,
  SAFE_CAST(TRIM(SAFE_CAST(unidade_medida AS STRING)) AS STRING) AS unidade_medida,
  SAFE_CAST(TRIM(SAFE_CAST(observacao AS STRING)) AS STRING) AS observacao
FROM {{ source('brutos_osinfo_staging', 'itens_nota_fiscal') }} AS t
