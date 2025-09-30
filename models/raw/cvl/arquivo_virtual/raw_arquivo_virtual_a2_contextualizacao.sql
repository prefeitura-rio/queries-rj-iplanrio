{{
    config(
        alias='a2_contextualizacao'
    )
}}

SELECT
    safe_cast(ds_historiaAdm as string) as historia_administrativa,
    safe_cast(ds_historiaArq as string) as historia_arquivistica,
    safe_cast(id_NaturezaJuridica as int64) as codigo_natureza_juridica,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_arquivo_virtual_staging', 'a2_contextualizacao') }}
