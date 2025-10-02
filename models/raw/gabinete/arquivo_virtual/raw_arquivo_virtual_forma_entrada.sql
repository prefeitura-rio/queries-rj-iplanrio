{{
    config(
        alias='forma_aquisicao_acervo'
    )
}}

SELECT
    safe_cast(id_formaEntrada as int64) as codigo_forma_entrada,
    safe_cast(ds_formaEntrada as string) as forma_entrada,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'forma_entrada') }}
