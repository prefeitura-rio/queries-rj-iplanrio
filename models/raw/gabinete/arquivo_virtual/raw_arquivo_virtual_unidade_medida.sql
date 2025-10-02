{{
    config(
        alias='unidade_medida'
    )
}}

SELECT
    safe_cast(id_unidade as int64) as codigo_unidade,
    safe_cast(nm_unidade as string) as unidade_medida,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'unidade_medida') }}
