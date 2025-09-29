{{
    config(
        alias='restricao'
    )
}}

SELECT
    safe_cast(id_restricao as int64) as id_restricao,
    safe_cast(ds_restricaobasica as string) as ds_restricaobasica,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'restricao') }}
