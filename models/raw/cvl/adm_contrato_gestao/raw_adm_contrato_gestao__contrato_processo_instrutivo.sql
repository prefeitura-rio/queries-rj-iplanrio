{{
    config(
        alias='contrato_processo_instrutivo',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(id_contrato), r'\.0$', '') AS INT64) AS id_contrato,
  SAFE_CAST(TRIM(cod_orgao) AS INT64) AS id_orgao,
  SAFE_CAST(TRIM(processo_instrutivo) AS STRING) AS processo_instrutivo,
  SAFE_CAST(TRIM(cod_unidade) AS INT64) AS id_unidade
FROM {{ source('brutos_osinfo_staging', 'contrato_processo_instrutivo') }} AS t
 