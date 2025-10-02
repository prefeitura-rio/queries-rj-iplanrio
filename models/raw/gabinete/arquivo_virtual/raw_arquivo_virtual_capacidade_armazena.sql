{{
    config(
        alias='capacidade_armazenamento'
    )
}}

SELECT
    safe_cast(Id_capacidade as int64) as codigo_capacidade,
    safe_cast(ds_capacidade as string) as descricao_capacidade_armazenamento,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'capacidade_armazena') }}
