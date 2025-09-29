{{
    config(
        alias='natureza_juridica'
    )
}}

SELECT
    safe_cast(id_NaturezaJuridica as int64) as id_NaturezaJuridica,
    safe_cast(ds_natureza as string) as ds_natureza,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'natureza_juridica') }}
