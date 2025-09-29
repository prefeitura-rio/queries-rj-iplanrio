{{
    config(
        alias='nivel'
    )
}}

SELECT
    safe_cast(id_nivel as int64) as id_nivel,
    safe_cast(ds_nivel as string) as ds_nivel,
    safe_cast(nr_nivel as int64) as nr_nivel,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'nivel') }}
