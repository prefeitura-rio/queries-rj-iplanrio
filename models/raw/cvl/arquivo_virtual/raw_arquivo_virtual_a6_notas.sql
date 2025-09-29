{{
    config(
        alias='a6_notas'
    )
}}

SELECT
    safe_cast(ds_notaConservacao as string) as ds_notaConservacao,
    safe_cast(id_estadoAcervo as int64) as id_estadoAcervo,
    safe_cast(ds_notasGerais as string) as ds_notasGerais,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at                                
FROM {{ source('brutos_arquivo_virtual_staging', 'a6_notas') }}
