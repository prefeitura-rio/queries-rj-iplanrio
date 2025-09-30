{{
    config(
        alias='conjunto_arquivo'
    )
}}

SELECT
    safe_cast(id_nivel as int64) as codigo_nivel,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    safe_cast(id_conjuntoArquivo_pai as int64) as codigo_conjunto_arquivo_pai,
    safe_cast(st_conjuntoArquivo as int64) as status_conjunto_arquivo,
    safe_cast(st_conjuntoArquivo_pai as int64) as status_conjunto_arquivo_pai,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'conjunto_arquivo') }}
