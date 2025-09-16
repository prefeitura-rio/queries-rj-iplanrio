{{
    config(
        alias='tipo_escala'
    )
}}

SELECT
    safe_cast(id_tipoEscala as int64) as id_tipoEscala,
    safe_cast(ds_tipoEscala as string) as ds_tipoEscala,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'tipo_escala') }}
