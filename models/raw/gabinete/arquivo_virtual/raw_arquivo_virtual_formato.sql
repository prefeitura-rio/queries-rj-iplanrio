{{
    config(
        alias='formato_fisico_acervo'
    )
}}

SELECT
    safe_cast(id_formato as int64) as codigo_formato,
    safe_cast(id_genero as int64) as codigo_genero,
    safe_cast(ds_formato as string) as formato,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'formato') }}
