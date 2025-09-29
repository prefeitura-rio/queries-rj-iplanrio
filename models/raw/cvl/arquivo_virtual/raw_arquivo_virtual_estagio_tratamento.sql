{{
    config(
        alias='estagio_tratamento'
    )
}}

SELECT
    safe_cast(id_estagioTratamento as int64) as id_estagioTratamento,
    safe_cast(ds_estagioTratamento as string) as ds_estagioTratamento,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'estagio_tratamento') }}
