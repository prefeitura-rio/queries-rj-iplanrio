{{
    config(
        alias='conjunto_arquivo'
    )
}}

SELECT
    safe_cast(id_nivel as int64) as id_nivel,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    safe_cast(id_conjuntoArquivo_pai as int64) as id_conjuntoArquivo_pai,
    safe_cast(st_conjuntoArquivo as string) as st_conjuntoArquivo,
    safe_cast(st_conjuntoArquivo_pai as string) as st_conjuntoArquivo_pai,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'conjunto_arquivo') }}
