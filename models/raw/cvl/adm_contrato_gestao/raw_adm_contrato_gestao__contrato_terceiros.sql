{{
    config(
        alias='contrato_terceiros',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}


SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato_terceiro AS STRING)), r'\.0$', '') AS STRING) AS id_contrato_terceiro,
  SAFE_CAST(TRIM(SAFE_CAST(codigo_organizacao AS STRING)) AS STRING) AS cod_organizacao,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato AS STRING)), r'\.0$', '') AS STRING) AS id_instrumento_contratual,
  SAFE_CAST(TRIM(SAFE_CAST(valor_mes AS STRING)) AS NUMERIC) AS valor_mes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(contrato_mes_inicio AS STRING)), r'\.0$', '') AS INT64) AS contrato_mes_inicio,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(contrato_ano_inicio AS STRING)), r'\.0$', '') AS INT64) AS contrato_ano_inicio,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(contrato_mes_fim AS STRING)), r'\.0$', '') AS INT64) AS contrato_mes_fim,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(contrato_ano_fim AS STRING)), r'\.0$', '') AS INT64) AS contrato_ano_fim,
  SAFE_CAST(TRIM(SAFE_CAST(cod_unidade AS STRING)) AS STRING) AS cod_unidade,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(referencia_ano_ass_contrato AS STRING)), r'\.0$', '') AS INT64) AS referencia_ano_ass_contrato,
  SAFE_CAST(TRIM(SAFE_CAST(vigencia AS STRING)) AS STRING) AS vigencia,
  SAFE_CAST(TRIM(SAFE_CAST(cnpj AS STRING)) AS STRING) AS cnpj,
  SAFE_CAST(TRIM(SAFE_CAST(razao_social AS STRING)) AS STRING) AS razao_social,
  SAFE_CAST(TRIM(SAFE_CAST(servico AS STRING)) AS STRING) AS servico,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(referencia_mes_receita AS STRING)), r'\.0$', '') AS INT64) AS referencia_mes_receita,
  SAFE_CAST(TRIM(SAFE_CAST(flg_imagem AS STRING)) AS STRING) AS flg_imagem,
  SAFE_CAST(TRIM(SAFE_CAST(imagem_contrato AS STRING)) AS STRING) AS imagem_contrato
FROM {{ source('brutos_osinfo_staging', 'contrato_terceiros') }} AS t
