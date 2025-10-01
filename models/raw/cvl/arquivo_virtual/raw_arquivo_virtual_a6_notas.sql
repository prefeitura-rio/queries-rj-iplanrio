{{
    config(
        alias='notas_conservacao'
    )
}}

SELECT
    safe_cast(ds_notaConservacao as string) as nota_conservacao,
    safe_cast(id_estadoAcervo as int64) as codigo_estado_acervo,
    safe_cast(ds_notasGerais as string) as notas_gerais,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at                                
FROM {{ source('brutos_arquivo_virtual_staging', 'a6_notas') }}
