{{
    config(
      alias="dc_tipo_vinculacao",
      description="Tipos de vinculação empregatícia que classificam as diferentes formas de contrato de trabalho."
    )
}}

select
    safe_cast(`TPVC_CD_TIPO_VINCULACAO` as integer) as tipo_vinculacao_codigo,
    safe_cast(`TPVC_DS_DESCRICAO` as string) as tipo_vinculacao_descricao,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('os_info_rh', 'dc_tipo_vinculacao') }}
