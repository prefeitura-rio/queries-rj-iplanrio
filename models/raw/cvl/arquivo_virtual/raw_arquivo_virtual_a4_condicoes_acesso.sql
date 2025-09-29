{{
    config(
        alias='a4_condicoes_acesso'
    )
}}

SELECT
    safe_cast(ds_condicoesReproducao as string) as ds_condicoesReproducao,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo, 
    safe_cast(obs_restricao as string) as obs_restricao,  
    safe_cast(id_restricao as int64) as id_restricao,
    safe_cast(id_tipoRestricao as int64) as id_tipoRestricao,
       _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_arquivo_virtual_staging', 'a4_condicoes_acesso') }}
