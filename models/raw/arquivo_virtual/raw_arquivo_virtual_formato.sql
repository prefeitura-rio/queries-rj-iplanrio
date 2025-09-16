{{
    config(
        alias='formato'
    )
}}

SELECT
    safe_cast(id_formato as int64) as id_formato,
    safe_cast(id_genero as int64) as id_genero,
    safe_cast(ds_formato as string) as ds_formato,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'formato') }}
