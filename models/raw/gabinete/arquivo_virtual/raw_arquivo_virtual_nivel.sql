{{
    config(
        alias='nivel_hierarquia'
    )
}}

SELECT
    safe_cast(id_nivel as int64) as codigo_nivel,
    safe_cast(ds_nivel as string) as nivel,
    safe_cast(nr_nivel as int64) as numero_nivel,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'nivel') }}
