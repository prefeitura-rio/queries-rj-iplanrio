{{
    config(
        alias='especie'
    )
}}

SELECT
    safe_cast(id_especie as int64) as id_especie,
    safe_cast(id_genero as int64) as id_genero,
    safe_cast(nm_especie as string) as nm_especie,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'especie') }}
