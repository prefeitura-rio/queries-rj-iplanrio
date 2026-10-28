{{
    config(
        alias='tipo_entrega',
        schema='adm_contrato_gestao',

    )
}}

SELECT
  SAFE_CAST(tiet_cd_tipo_entrega AS INT64) AS id_tipo_entrega,
  SAFE_CAST(TRIM(SAFE_CAST(tiet_ds_tipo_entrega AS STRING)) AS STRING) AS descricao_tipo_entrega,
  SAFE_CAST(TRIM(SAFE_CAST(tiet_tabela_tipo_entrega AS STRING)) AS STRING) AS tabela_tipo_entrega
FROM {{ source('brutos_osinfo_staging', 'tb_tipo_entrega') }} AS t
