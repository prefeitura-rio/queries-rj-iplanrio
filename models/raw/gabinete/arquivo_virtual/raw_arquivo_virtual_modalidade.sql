{{
    config(
        alias='modalidade_identificacao'
    )
}}

SELECT
    safe_cast(id_modalidade as int64) as codigo_modalidade,
    safe_cast(ds_modalidade as string) as modalidade,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'modalidade') }}
