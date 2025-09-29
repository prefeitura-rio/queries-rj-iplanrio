{{
    config(
        alias='capacidade_armazena'
    )
}}

SELECT
    safe_cast(Id_capacidade as int64) as Id_capacidade,
    safe_cast(ds_capacidade as string) as ds_capacidade,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'capacidade_armazena') }}
