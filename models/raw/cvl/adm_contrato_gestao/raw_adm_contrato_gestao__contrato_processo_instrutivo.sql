{{
    config(
        alias='contrato_processo_instrutivo',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_contrato', 'int64', trim=true) }} AS id_contrato,
  SAFE_CAST(TRIM(cod_orgao) AS INT64) AS id_orgao,
  SAFE_CAST(TRIM(processo_instrutivo) AS STRING) AS processo_instrutivo,
  SAFE_CAST(TRIM(cod_unidade) AS INT64) AS id_unidade
FROM {{ source('brutos_osinfo_staging', 'contrato_processo_instrutivo') }} AS t
 