{{
    config(
        alias='condicao_instrumentos_pesquisa'
    )
}}

SELECT
    safe_cast(id_condicInst as int64) as codigo_condicao_instr,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    safe_cast(id_instPesquisa as int64) as codigo_instrumento_pesquisa,
    safe_cast(ds_condicInst as string) as condicao_instr,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'condic_inst') }}
