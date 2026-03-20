{{
    config(
        alias='saldo_dados',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_saldo_dados', 'string', trim=true) }} AS id_saldo_dados,
  {{ clean_and_cast('id_saldo_item', 'string', trim=true) }} AS id_saldo_item,
  {{ clean_and_cast('referencia_mes_receita', 'int64', trim=true) }} AS referencia_mes_receita,
  {{ clean_and_cast('referencia_ano_receita', 'int64', trim=true) }} AS referencia_ano_receita,
  SAFE_CAST(TRIM(valor) AS NUMERIC) AS valor,
  SAFE_CAST(TRIM(flg_ativo) AS STRING) AS flg_ativo,
  {{ clean_and_cast('id_contrato', 'string', trim=true) }} AS id_instrumento_contratual,
  {{ clean_and_cast('id_conta_bancaria', 'string', trim=true) }} AS id_conta_bancaria,
  SAFE_CAST(TRIM(arq_img_ext) AS STRING) AS arq_img_ext
FROM {{ source('brutos_osinfo_staging', 'saldo_dados') }} AS t