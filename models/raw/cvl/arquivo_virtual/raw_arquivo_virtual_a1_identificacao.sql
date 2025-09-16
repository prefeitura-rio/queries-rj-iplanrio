{{
    config(
        alias='a1_identificacao'
    )
}}


SELECT
    safe_cast(cd_referencia as string) as cd_referencia,
    safe_cast(dt_fim as datetime) as dt_fim,
    safe_cast(dt_iniModalidade as datetime) as dt_iniModalidade,
    safe_cast(dt_pesquisa as datetime) as dt_pesquisa,
    safe_cast(ds_titulo as string) as ds_titulo,
    safe_cast(id_modalidade as int64) as id_modalidade,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    safe_cast(id_setor as int64) as id_setor,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_arquivo_virtual_staging', 'a1_identificacao') }}
