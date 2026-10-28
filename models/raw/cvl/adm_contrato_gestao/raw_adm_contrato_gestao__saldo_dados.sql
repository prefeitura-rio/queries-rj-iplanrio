{{
    config(
        alias='saldo_dados',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_saldo_dados AS STRING)), r'\.0$', '') AS STRING) AS id_saldo_dados,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_saldo_item AS STRING)), r'\.0$', '') AS STRING) AS id_saldo_item,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(referencia_mes_receita AS STRING)), r'\.0$', '') AS INT64) AS referencia_mes_receita,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(referencia_ano_receita AS STRING)), r'\.0$', '') AS INT64) AS referencia_ano_receita,
  SAFE_CAST(TRIM(SAFE_CAST(valor AS STRING)) AS NUMERIC) AS valor,
  SAFE_CAST(TRIM(SAFE_CAST(flg_ativo AS STRING)) AS STRING) AS flg_ativo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato AS STRING)), r'\.0$', '') AS STRING) AS id_instrumento_contratual,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_conta_bancaria AS STRING)), r'\.0$', '') AS STRING) AS id_conta_bancaria,
  SAFE_CAST(TRIM(SAFE_CAST(arq_img_ext AS STRING)) AS STRING) AS arq_img_ext
FROM {{ source('brutos_osinfo_staging', 'saldo_dados') }} AS t
