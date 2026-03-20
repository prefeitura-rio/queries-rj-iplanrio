{{
    config(
        alias='receita_dados',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_receita_dados', 'string', trim=true) }} AS id_receita_dados,
  SAFE_CAST(TRIM(cod_unidade) AS STRING) AS cod_unidade,
  {{ clean_and_cast('id_item', 'string', trim=true) }} AS id_item,
  {{ clean_and_cast('referencia_mes', 'int64', trim=true) }} AS referencia_mes,
  {{ clean_and_cast('referencia_ano', 'int64', trim=true) }} AS referencia_ano,
  SAFE_CAST(TRIM(valor) AS NUMERIC) AS valor,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo,
  {{ clean_and_cast('id_contrato', 'string', trim=true) }} AS id_instrumento_contratual,
  {{ clean_and_cast('id_termo_aditivo', 'string', trim=true) }} AS id_termo_aditivo,
  {{ clean_and_cast('id_conta_bancaria', 'string', trim=true) }} AS id_conta_bancaria
FROM {{ source('brutos_osinfo_staging', 'receita_dados') }} AS t