{{
    config(
        alias='contexto_produtor'
    )
}}

SELECT
    safe_cast(id_produtor as int64) as codigo_produtor,
    safe_cast(nm_produtor as string) as nome_produtor,
    safe_cast(dt_morteExtincao as datetime) as data_morte_extincao,
    safe_cast(dt_nascimentoCriacao as datetime) as data_nascimento_criacao,
    safe_cast(id_tipoProdutor as int64) as codigo_tipo_produtor,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'context_produtor') }}
