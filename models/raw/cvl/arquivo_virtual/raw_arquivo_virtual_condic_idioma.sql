{{
    config(
        alias='condicao_arquivos_idiomas'
    )
}}

SELECT
    safe_cast(id_idioma as int64) as codigo_idioma,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'condic_idioma') }}
