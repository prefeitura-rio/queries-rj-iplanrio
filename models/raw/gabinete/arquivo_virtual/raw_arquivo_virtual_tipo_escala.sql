{{
    config(
        alias='tipo_escala'
    )
}}

SELECT
    safe_cast(id_tipoEscala as int64) as codigo_tipo_escala,
    safe_cast(ds_tipoEscala as string) as tipo_escala,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'tipo_escala') }}
