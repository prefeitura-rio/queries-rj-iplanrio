{{
    config(
        alias='condic_inst'
    )
}}

SELECT
    safe_cast(id_condicInst as int64) as id_condicInst,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    safe_cast(id_instPesquisa as int64) as id_instPesquisa,
    safe_cast(ds_condicInst as string) as ds_condicInst,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'condic_inst') }}
