{{
    config(
      alias="tipo_vinculacao",
      description="Tipos de vinculação empregatícia que classificam as diferentes formas de contrato de trabalho.",
      materialized='table'
    )
}}

select
    safe_cast(`TPVC_CD_TIPO_VINCULACAO` as string) as tipo_vinculacao_codigo,
    safe_cast(`TPVC_DS_DESCRICAO` as string) as tipo_vinculacao_descricao,
    DATETIME(_prefect_extracted_at, "America/Sao_Paulo") AS datalake_loaded_at,
    DATETIME(current_timestamp(), "America/Sao_Paulo") AS datalake_transformed_at
FROM {{ source('brutos_osinfo_rh_staging', 'dc_tipo_vinculacao') }}
