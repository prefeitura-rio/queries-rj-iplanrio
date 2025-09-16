{{
    config(
        alias='context_produtor'
    )
}}

SELECT
    safe_cast(id_produtor as int64) as id_produtor,
    safe_cast(nm_produtor as string) as nm_produtor,
    safe_cast(dt_morteExtincao as datetime) as dt_morteExtincao,
    safe_cast(dt_nascimentoCriacao as datetime) as dt_nascimentoCriacao,
    safe_cast(id_tipoProdutor as int64) as id_tipoProdutor,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'context_produtor') }}
