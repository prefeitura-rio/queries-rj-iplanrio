SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_item_nf), r'\.0$', '') AS STRING) AS id_item_nf,
  SAFE_CAST(TRIM(cod_item_nf) AS STRING) AS cod_item_nf,
  SAFE_CAST(TRIM(qtd_material) AS NUMERIC) AS qtd_material,
  SAFE_CAST(TRIM(valor_unitario) AS NUMERIC) AS valor_unitario,
  SAFE_CAST(REGEXP_REPLACE(TRIM(referencia_mes_nf), r'\.0$', '') AS INT64) AS referencia_mes_nf,
  SAFE_CAST(REGEXP_REPLACE(TRIM(referencia_ano_nf), r'\.0$', '') AS INT64) AS referencia_ano_nf,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_fornecedor), r'\.0$', '') AS STRING) AS id_fornecedor,
  SAFE_CAST(TRIM(valor_total) AS NUMERIC) AS valor_total,
  SAFE_CAST(TRIM(num_documento) AS STRING) AS num_documento,
  SAFE_CAST(TRIM(cod_organizacao) AS STRING) AS cod_instituicao,
  SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', data_envio) AS DATETIME) AS data_envio,
  SAFE_CAST(TRIM(tipo_item) AS STRING) AS tipo_item,
  SAFE_CAST(TRIM(item) AS STRING) AS item,
  SAFE_CAST(TRIM(unidade_medida) AS STRING) AS unidade_medida,
  SAFE_CAST(TRIM(observacao) AS STRING) AS observacao
FROM {{ source('brutos_osinfo_staging', 'itens_nota_fiscal') }}