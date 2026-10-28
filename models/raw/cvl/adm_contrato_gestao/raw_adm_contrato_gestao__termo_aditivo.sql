{{
    config(
        alias='termo_aditivo',
        schema='adm_contrato_gestao',
        materialized='table',
        project= 'rj-cvl'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_termo_aditivo AS STRING)), r'\.0$', '') AS INT64) AS id_termo_aditivo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato AS STRING)), r'\.0$', '') AS INT64) AS id_contrato,
  SAFE_CAST(TRIM(SAFE_CAST(num_termo_aditivo AS STRING)) AS STRING) AS num_termo_aditivo,
  SAFE_CAST(TRIM(SAFE_CAST(cod_os AS STRING)) AS INT64) AS id_os,
  SAFE_CAST(DATE(dt_atualizacao) AS DATE) AS data_atualizacao,
  SAFE_CAST(DATE(dt_assinatura) AS DATE) AS data_assinatura,
  SAFE_CAST(TRIM(SAFE_CAST(periodo_vigencia AS STRING)) AS STRING) AS periodo_vigencia,
  SAFE_CAST(DATE(dt_publicacao) AS DATE) AS data_publicacao,
  SAFE_CAST(DATE(dt_inicio) AS DATE) AS data_inicio,
  SAFE_CAST(TRIM(SAFE_CAST(vlr_inicial AS STRING)) AS NUMERIC) AS valor_inicial,
  SAFE_CAST(TRIM(SAFE_CAST(vlr_alterado AS STRING)) AS NUMERIC) AS valor_alterado,
  SAFE_CAST(TRIM(SAFE_CAST(observacao AS STRING)) AS STRING) AS observacao
FROM {{ source('brutos_osinfo_staging', 'termo_aditivo') }} AS t
