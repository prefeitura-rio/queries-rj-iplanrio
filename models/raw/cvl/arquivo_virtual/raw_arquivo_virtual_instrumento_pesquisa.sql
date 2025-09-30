{{
    config(
        alias='instrumento_pesquisa'
    )
}}

SELECT
    safe_cast(id_instPesquisa as int64) as codigo_instrumento,
    safe_cast(ds_instrumento as string) as nome_instrumento,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'instrumento_pesquisa') }}
