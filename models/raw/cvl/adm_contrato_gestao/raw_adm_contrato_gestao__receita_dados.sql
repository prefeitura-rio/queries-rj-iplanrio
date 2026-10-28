{{
    config(
        alias='receita_dados',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_receita_dados AS STRING)), r'\.0$', '') AS STRING) AS id_receita_dados,
  SAFE_CAST(TRIM(SAFE_CAST(cod_unidade AS STRING)) AS STRING) AS cod_unidade,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_item AS STRING)), r'\.0$', '') AS STRING) AS id_item,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(referencia_mes AS STRING)), r'\.0$', '') AS INT64) AS referencia_mes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(referencia_ano AS STRING)), r'\.0$', '') AS INT64) AS referencia_ano,
  SAFE_CAST(TRIM(SAFE_CAST(valor AS STRING)) AS NUMERIC) AS valor,
  SAFE_CAST(TRIM(SAFE_CAST(flg_ativo AS STRING)) AS STRING) AS flg_ativo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato AS STRING)), r'\.0$', '') AS STRING) AS id_instrumento_contratual,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_termo_aditivo AS STRING)), r'\.0$', '') AS STRING) AS id_termo_aditivo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_conta_bancaria AS STRING)), r'\.0$', '') AS STRING) AS id_conta_bancaria
FROM {{ source('brutos_osinfo_staging', 'receita_dados') }} AS t
