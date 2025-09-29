{{
    config(
        alias='tipo_restricao'
    )
}}

SELECT
    safe_cast(id_tipoRestricao as int64) as id_tipoRestricao,
    safe_cast(ds_restricao as string) as ds_restricao,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'tipo_restricao') }}
