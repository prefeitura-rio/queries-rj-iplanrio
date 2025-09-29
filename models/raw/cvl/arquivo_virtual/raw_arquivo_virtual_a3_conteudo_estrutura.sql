{{
    config(
        alias='a3_conteudo_estrutura'
    )
}}

SELECT
    safe_cast(ds_incorporacoes as string) as ds_incorporacoes,
    safe_cast(ds_avalEliminaTemp as string) as ds_avalEliminaTemp,
    safe_cast(ds_ambitoConteudo as string) as ds_ambitoConteudo,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    safe_cast(ds_organizacao as string) as ds_organizacao,
    safe_cast(id_estagioTratamento as int64) as id_estagioTratamento,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_arquivo_virtual_staging', 'a3_conteudo_estrutura') }}
