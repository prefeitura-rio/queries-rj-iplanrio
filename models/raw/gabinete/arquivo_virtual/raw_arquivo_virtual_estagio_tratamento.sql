{{
    config(
        alias='estagio_tratamento'
    )
}}

SELECT
    safe_cast(id_estagioTratamento as int64) as codigo_estagio_tratamento,
    safe_cast(ds_estagioTratamento as string) as estagio_tratamento,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'estagio_tratamento') }}
