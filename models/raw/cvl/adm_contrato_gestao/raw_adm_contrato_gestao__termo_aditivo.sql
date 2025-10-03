{{
    config(
        alias='termo_aditivo',
        schema='adm_contrato_gestao',
        materialized='table',
        project= 'rj-cvl'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_termo_aditivo), r'\.0$', '') AS INT64) AS id_termo_aditivo,
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato), r'\.0$', '') AS INT64) AS id_contrato,
  SAFE_CAST(TRIM(num_termo_aditivo) AS STRING) AS num_termo_aditivo,
  SAFE_CAST(TRIM(cod_os) AS INT64) AS id_os,
  SAFE_CAST(DATE(dt_atualizacao) AS DATE) AS data_atualizacao,
  SAFE_CAST(DATE(dt_assinatura) AS DATE) AS data_assinatura,
  SAFE_CAST(TRIM(periodo_vigencia) AS STRING) AS periodo_vigencia,
  SAFE_CAST(DATE(dt_publicacao) AS DATE) AS data_publicacao,
  SAFE_CAST(DATE(dt_inicio) AS DATE) AS data_inicio,
  SAFE_CAST(TRIM(vlr_inicial) AS NUMERIC) AS valor_inicial,
  SAFE_CAST(TRIM(vlr_alterado) AS NUMERIC) AS valor_alterado,
  SAFE_CAST(TRIM(observacao) AS STRING) AS observacao
FROM {{ source('brutos_osinfo_staging', 'termo_aditivo') }} AS t
