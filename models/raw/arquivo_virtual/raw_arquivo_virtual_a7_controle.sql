{{
    config(
        alias='a7_controle'
    )
}}

SELECT
    safe_cast(ds_datasDesc as string) as ds_datasDesc,
    safe_cast(ds_regrasConvencoes as string) as ds_regrasConvencoes,
    safe_cast(ds_notaArquivistica as string) as ds_notaArquivistica,
    safe_cast(ds_unidadeCustodiadora as string) as ds_unidadeCustodiadora,
    safe_cast(st_arquivoDigital as string) as st_arquivoDigital,
    safe_cast(ds_responsavelDesc as string) as ds_responsavelDesc,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'a7_controle') }}
