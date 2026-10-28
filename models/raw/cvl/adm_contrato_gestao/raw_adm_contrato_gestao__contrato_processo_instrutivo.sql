{{
    config(
        alias='contrato_processo_instrutivo',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_contrato AS STRING)), r'\.0$', '') AS INT64) AS id_contrato,
  SAFE_CAST(TRIM(SAFE_CAST(cod_orgao AS STRING)) AS INT64) AS id_orgao,
  SAFE_CAST(TRIM(SAFE_CAST(processo_instrutivo AS STRING)) AS STRING) AS processo_instrutivo,
  SAFE_CAST(TRIM(SAFE_CAST(cod_unidade AS STRING)) AS INT64) AS id_unidade
FROM {{ source('brutos_osinfo_staging', 'contrato_processo_instrutivo') }} AS t
 