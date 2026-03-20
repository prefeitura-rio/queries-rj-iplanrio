{{
    config(
        alias='tipo_arquivo',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  {{ clean_and_cast('id_tipo_arquivo', 'string', trim=true) }} AS id_tipo_arquivo,
  SAFE_CAST(TRIM(tipo_servico) AS STRING) AS tipo_servico,
  SAFE_CAST(TRIM(extensao) AS STRING) AS extensao,
  SAFE_CAST(TRIM(flg_atividade) AS STRING) AS flg_atividade
FROM {{ source('brutos_osinfo_staging', 'tipo_arquivo') }} AS t