{{
    config(
        alias='forma_entrada'
    )
}}

SELECT
    safe_cast(id_formaEntrada as int64) as id_formaEntrada,
    safe_cast(ds_formaEntrada as string) as ds_formaEntrada,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'forma_entrada') }}
