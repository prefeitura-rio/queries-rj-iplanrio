{{
    config(
        alias='a1_identificacao'
    )
}}


SELECT
    safe_cast(cd_referencia as string) as codigo_referencia,
    safe_cast(dt_fim as string) as data_fim,
    safe_cast(dt_iniModalidade as string) as data_inicio_modalidade,
    safe_cast(dt_pesquisa as string) as data_pesquisa,
    safe_cast(ds_titulo as string) as descricao_titulo,
    safe_cast(id_modalidade as int64) as codigo_modalidade,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    safe_cast(id_setor as int64) as codigo_setor,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_arquivo_virtual_staging', 'a1_identificacao') }}
