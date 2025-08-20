{{
    config(
        alias='contrato',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato), r'\.0$', '') AS STRING) AS id_contrato,
  SAFE_CAST(TRIM(numero_contrato) AS STRING) AS numero_contrato,
  SAFE_CAST(TRIM(cod_organizacao) AS STRING) AS cod_organizacao,
  SAFE_CAST(DATE(data_atualizacao) AS DATE) AS data_atualizacao,
  SAFE_CAST(DATE(data_assinatura) AS DATE) AS data_assinatura,
  SAFE_CAST(TRIM(periodo_vigencia) AS STRING) AS periodo_vigencia,
  SAFE_CAST(DATE(data_publicacao) AS DATE) AS data_publicacao,
  SAFE_CAST(DATE(data_inicio) AS DATE) AS data_inicio,
  SAFE_CAST(TRIM(valor_total) AS NUMERIC) AS valor_total,
  SAFE_CAST(TRIM(valor_ano1) AS NUMERIC) AS valor_ano1,
  SAFE_CAST(TRIM(valor_parcelas) AS NUMERIC) AS valor_parcelas,
  SAFE_CAST(TRIM(valor_fixo) AS NUMERIC) AS valor_fixo,
  SAFE_CAST(TRIM(valor_variavel) AS NUMERIC) AS valor_variavel,
  SAFE_CAST(TRIM(observacao) AS STRING) AS observacao,
  SAFE_CAST(TRIM(ap) AS STRING) AS ap,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_secretaria), r'\.0$', '') AS STRING) AS id_secretaria
FROM {{ source('brutos_osinfo_staging', 'contrato') }}