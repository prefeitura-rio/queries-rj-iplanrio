{{
    config(
        alias='conteudo_estrutura'
    )
}}

SELECT
    safe_cast(ds_incorporacoes as string) as incorporacoes,
    safe_cast(ds_avalEliminaTemp as string) as avaliacao_eliminacao_temporal,
    safe_cast(ds_ambitoConteudo as string) as ambito_conteudo,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    safe_cast(ds_organizacao as string) as organizacao,
    safe_cast(id_estagioTratamento as int64) as codigo_estagio_tratamento,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_arquivo_virtual_staging', 'a3_conteudo_estrutura') }}
