{{
    config(
        alias='genero'
    )
}}

SELECT
    safe_cast(id_genero as int64) as id_genero,
    safe_cast(nm_genero as string) as nm_genero,
    safe_cast(nr_genero as int64) as nr_genero,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'genero') }}
