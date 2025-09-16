{{
    config(
        alias='condic_idioma'
    )
}}

SELECT
    safe_cast(id_idioma as int64) as id_idioma,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'condic_idioma') }}
