{{
    config(
        alias='tipo_produtor'
    )
}}

SELECT
    safe_cast(id_tipoProdutor as int64) as id_tipoProdutor,
    safe_cast(ds_tipoProdutor as string) as ds_tipoProdutor,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'tipo_produtor') }}
