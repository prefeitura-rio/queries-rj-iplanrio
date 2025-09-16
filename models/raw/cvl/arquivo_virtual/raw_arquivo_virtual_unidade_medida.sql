{{
    config(
        alias='unidade_medida'
    )
}}

SELECT
    safe_cast(id_unidade as int64) as id_unidade,
    safe_cast(nm_unidade as string) as nm_unidade,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'unidade_medida') }}
