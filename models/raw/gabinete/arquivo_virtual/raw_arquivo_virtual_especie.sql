{{
    config(
        alias='especie_arquivo'
    )
}}

SELECT
    safe_cast(id_especie as int64) as codigo_especie,
    safe_cast(id_genero as int64) as codigo_genero,
    safe_cast(nm_especie as string) as nome_especie,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'especie') }}
