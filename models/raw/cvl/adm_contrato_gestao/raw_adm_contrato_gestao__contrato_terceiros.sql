{{
    config(
        alias='contrato_terceiros',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}


SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato_terceiro), r'\.0$', '') AS STRING) AS id_contrato_terceiro,
  SAFE_CAST(TRIM(codigo_organizacao) AS STRING) AS cod_organizacao,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato), r'\.0$', '') AS STRING) AS id_instrumento_contratual,
  SAFE_CAST(TRIM(valor_mes) AS NUMERIC) AS valor_mes,
  SAFE_CAST(REGEXP_REPLACE(TRIM(contrato_mes_inicio), r'\.0$', '') AS INT64) AS contrato_mes_inicio,
  SAFE_CAST(REGEXP_REPLACE(TRIM(contrato_ano_inicio), r'\.0$', '') AS INT64) AS contrato_ano_inicio,
  SAFE_CAST(REGEXP_REPLACE(TRIM(contrato_mes_fim), r'\.0$', '') AS INT64) AS contrato_mes_fim,
  SAFE_CAST(REGEXP_REPLACE(TRIM(contrato_ano_fim), r'\.0$', '') AS INT64) AS contrato_ano_fim,
  SAFE_CAST(TRIM(cod_unidade) AS STRING) AS cod_unidade,
  SAFE_CAST(REGEXP_REPLACE(TRIM(referencia_ano_ass_contrato), r'\.0$', '') AS INT64) AS referencia_ano_ass_contrato,
  SAFE_CAST(TRIM(vigencia) AS STRING) AS vigencia,
  SAFE_CAST(TRIM(cnpj) AS STRING) AS cnpj,
  SAFE_CAST(TRIM(razao_social) AS STRING) AS razao_social,
  SAFE_CAST(TRIM(servico) AS STRING) AS servico,
  SAFE_CAST(REGEXP_REPLACE(TRIM(referencia_mes_receita), r'\.0$', '') AS INT64) AS referencia_mes_receita,
  SAFE_CAST(TRIM(flg_imagem) AS STRING) AS flg_imagem,
  SAFE_CAST(TRIM(imagem_contrato) AS STRING) AS imagem_contrato
FROM {{ source('brutos_osinfo_staging', 'contrato_terceiros') }}