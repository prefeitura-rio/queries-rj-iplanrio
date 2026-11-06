{{
    config(
        alias='receita_dados',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_receita_dados), r'\.0$', '') AS STRING) AS id_receita_dados,
  SAFE_CAST(TRIM(cod_unidade) AS STRING) AS cod_unidade,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_item), r'\.0$', '') AS STRING) AS id_item,
  SAFE_CAST(REGEXP_REPLACE(TRIM(referencia_mes), r'\.0$', '') AS INT64) AS referencia_mes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(referencia_ano), r'\.0$', '') AS INT64) AS referencia_ano,
  SAFE_CAST(TRIM(valor) AS NUMERIC) AS valor,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato), r'\.0$', '') AS STRING) AS id_instrumento_contratual,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_termo_aditivo), r'\.0$', '') AS STRING) AS id_termo_aditivo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_conta_bancaria), r'\.0$', '') AS STRING) AS id_conta_bancaria
FROM {{ source('brutos_osinfo_staging', 'receita_dados') }} AS t