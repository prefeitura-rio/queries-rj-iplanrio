{{
    config(
        alias='usuario',
        schema='adm_contrato_gestao',
        materialized='table'
    )
}}

SELECT
  SAFE_CAST(TRIM(SAFE_CAST(cod_usuario AS STRING)) AS STRING) AS cod_usuario,
  SAFE_CAST(TRIM(SAFE_CAST(cod_unidade AS STRING)) AS STRING) AS cod_unidade,
  SAFE_CAST(TRIM(SAFE_CAST(login AS STRING)) AS STRING) AS login,
  SAFE_CAST(TRIM(SAFE_CAST(nome AS STRING)) AS STRING) AS nome,
  SAFE_CAST(DATE(data_cadastro) AS DATE) AS data_cadastro,
  SAFE_CAST(DATE(data_atualizacao) AS DATE) AS data_atualizacao,
  SAFE_CAST(DATE(data_exclusao) AS DATE) AS data_exclusao,
  SAFE_CAST(TRIM(SAFE_CAST(flg_excluido AS STRING)) AS STRING) AS flg_excluido,
  SAFE_CAST(TRIM(SAFE_CAST(cargo AS STRING)) AS STRING) AS cargo
FROM {{ source('brutos_osinfo_staging', 'usuario') }} AS t
