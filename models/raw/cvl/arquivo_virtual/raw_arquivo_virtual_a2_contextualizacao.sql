{{
    config(
        alias='a2_contextualizacao'
    )
}}

SELECT
    safe_cast(ds_historiaAdm as string) as ds_historiaAdm,
    safe_cast(ds_historiaArq as string) as ds_historiaArq,
    safe_cast(id_NaturezaJuridica as int64) as id_NaturezaJuridica,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_arquivo_virtual_staging', 'a2_contextualizacao') }}
