{{
    config(
      alias="tipo_vinculacao",
      description="Tipos de vinculação empregatícia que classificam as diferentes formas de contrato de trabalho."
    )
}}

select
    safe_cast(`TPVC_CD_TIPO_VINCULACAO` as string) as tipo_vinculacao_codigo,
    safe_cast(`TPVC_DS_DESCRICAO` as string) as tipo_vinculacao_descricao,
    safe_cast(_prefect_extracted_at AS datetime) AS datalake_loaded_at,
    safe_cast(current_timestamp() AS datetime) AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_tipo_vinculacao') }}
