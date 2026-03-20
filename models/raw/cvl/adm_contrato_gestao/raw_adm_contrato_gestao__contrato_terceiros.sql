{{
    config(
        alias='contrato_terceiros',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}


SELECT
  {{ clean_and_cast('id_contrato_terceiro', 'string', trim=true) }} AS id_contrato_terceiro,
  SAFE_CAST(TRIM(codigo_organizacao) AS STRING) AS cod_organizacao,
  {{ clean_and_cast('id_contrato', 'string', trim=true) }} AS id_instrumento_contratual,
  SAFE_CAST(TRIM(valor_mes) AS NUMERIC) AS valor_mes,
  {{ clean_and_cast('contrato_mes_inicio', 'int64', trim=true) }} AS contrato_mes_inicio,
  {{ clean_and_cast('contrato_ano_inicio', 'int64', trim=true) }} AS contrato_ano_inicio,
  {{ clean_and_cast('contrato_mes_fim', 'int64', trim=true) }} AS contrato_mes_fim,
  {{ clean_and_cast('contrato_ano_fim', 'int64', trim=true) }} AS contrato_ano_fim,
  SAFE_CAST(TRIM(cod_unidade) AS STRING) AS cod_unidade,
  {{ clean_and_cast('referencia_ano_ass_contrato', 'int64', trim=true) }} AS referencia_ano_ass_contrato,
  SAFE_CAST(TRIM(vigencia) AS STRING) AS vigencia,
  SAFE_CAST(TRIM(cnpj) AS STRING) AS cnpj,
  SAFE_CAST(TRIM(razao_social) AS STRING) AS razao_social,
  SAFE_CAST(TRIM(servico) AS STRING) AS servico,
  {{ clean_and_cast('referencia_mes_receita', 'int64', trim=true) }} AS referencia_mes_receita,
  SAFE_CAST(TRIM(flg_imagem) AS STRING) AS flg_imagem,
  SAFE_CAST(TRIM(imagem_contrato) AS STRING) AS imagem_contrato
FROM {{ source('brutos_osinfo_staging', 'contrato_terceiros') }} AS t