{{
    config(
        alias='idioma'
    )
}}

SELECT
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    safe_cast(id_idioma as int64) as id_idioma,
    safe_cast(nm_idioma as string) as nm_idioma,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'idioma') }}
