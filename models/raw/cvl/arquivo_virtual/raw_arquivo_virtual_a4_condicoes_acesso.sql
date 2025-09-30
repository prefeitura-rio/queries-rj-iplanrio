{{
    config(
        alias='a4_condicoes_acesso'
    )
}}

SELECT
    safe_cast(ds_condicoesReproducao as string) as condicoes_reproducao,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo, 
    safe_cast(obs as string) as observacoes,  
    safe_cast(id_restricao as int64) as codigo_restricao,
    safe_cast(id_tipoRestricao as int64) as codigo_tipo_restricao,
       _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_arquivo_virtual_staging', 'a4_condicoes_acesso') }}
