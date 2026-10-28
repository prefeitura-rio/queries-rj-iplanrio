{{
    config(
        alias='contrato',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato AS STRING)), r'\.0$', '') AS STRING) AS id_contrato,
  SAFE_CAST(TRIM(SAFE_CAST(numero_contrato AS STRING)) AS STRING) AS numero_contrato,
  SAFE_CAST(TRIM(SAFE_CAST(cod_organizacao AS STRING)) AS STRING) AS cod_organizacao,
  SAFE_CAST(DATE(data_atualizacao) AS DATE) AS data_atualizacao,
  SAFE_CAST(DATE(data_assinatura) AS DATE) AS data_assinatura,
  SAFE_CAST(TRIM(SAFE_CAST(periodo_vigencia AS STRING)) AS STRING) AS periodo_vigencia,
  SAFE_CAST(DATE(data_publicacao) AS DATE) AS data_publicacao,
  SAFE_CAST(DATE(data_inicio) AS DATE) AS data_inicio,
  SAFE_CAST(TRIM(SAFE_CAST(valor_total AS STRING)) AS NUMERIC) AS valor_total,
  SAFE_CAST(TRIM(SAFE_CAST(valor_ano1 AS STRING)) AS NUMERIC) AS valor_ano1,
  SAFE_CAST(TRIM(SAFE_CAST(valor_parcelas AS STRING)) AS NUMERIC) AS valor_parcelas,
  SAFE_CAST(TRIM(SAFE_CAST(valor_fixo AS STRING)) AS NUMERIC) AS valor_fixo,
  SAFE_CAST(TRIM(SAFE_CAST(valor_variavel AS STRING)) AS NUMERIC) AS valor_variavel,
  SAFE_CAST(TRIM(SAFE_CAST(observacao AS STRING)) AS STRING) AS observacao,
  SAFE_CAST(TRIM(SAFE_CAST(ap AS STRING)) AS STRING) AS ap,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_secretaria AS STRING)), r'\.0$', '') AS STRING) AS id_secretaria
FROM {{ source('brutos_osinfo_staging', 'contrato') }} AS t
