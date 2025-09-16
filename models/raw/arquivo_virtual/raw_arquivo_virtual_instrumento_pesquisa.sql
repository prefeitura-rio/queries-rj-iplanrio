{{
    config(
        alias='instrumento_pesquisa'
    )
}}

SELECT
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    safe_cast(id_genero as int64) as id_genero,
    safe_cast(id_instPesquisa as int64) as id_instPesquisa,
    safe_cast(ds_instrumento as string) as ds_instrumento,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'instrumento_pesquisa') }}
