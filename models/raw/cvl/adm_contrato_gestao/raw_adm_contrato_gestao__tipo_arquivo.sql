{{
    config(
        alias='tipo_arquivo',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(REGEXP_REPLACE(TRIM(SAFE_CAST(id_tipo_arquivo AS STRING)), r'\.0$', '') AS STRING) AS id_tipo_arquivo,
  SAFE_CAST(TRIM(SAFE_CAST(tipo_servico AS STRING)) AS STRING) AS tipo_servico,
  SAFE_CAST(TRIM(SAFE_CAST(extensao AS STRING)) AS STRING) AS extensao,
  SAFE_CAST(TRIM(SAFE_CAST(flg_atividade AS STRING)) AS STRING) AS flg_atividade
FROM {{ source('brutos_osinfo_staging', 'tipo_arquivo') }} AS t
