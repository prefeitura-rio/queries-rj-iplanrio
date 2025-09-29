{{
    config(
        alias='estado_acervo'
    )
}}

SELECT
    safe_cast(id_estadoAcervo as int64) as id_estadoAcervo,
    safe_cast(nm_estado as string) as nm_estado,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'estado_acervo') }}
