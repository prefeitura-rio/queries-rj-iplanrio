SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_saldo_dados), r'\.0$', '') AS STRING) AS id_saldo_dados,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_saldo_item), r'\.0$', '') AS STRING) AS id_saldo_item,
  SAFE_CAST(REGEXP_REPLACE(TRIM(referencia_mes_receita), r'\.0$', '') AS INT64) AS referencia_mes_receita,
  SAFE_CAST(REGEXP_REPLACE(TRIM(referencia_ano_receita), r'\.0$', '') AS INT64) AS referencia_ano_receita,
  SAFE_CAST(TRIM(valor) AS NUMERIC) AS valor,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato), r'\.0$', '') AS STRING) AS id_instrumento_contratual,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_conta_bancaria), r'\.0$', '') AS STRING) AS id_conta_bancaria,
  SAFE_CAST(TRIM(arq_img_ext) AS STRING) AS arq_img_ext
FROM {{ source('brutos_osinfo_staging', 'saldo_dados') }}