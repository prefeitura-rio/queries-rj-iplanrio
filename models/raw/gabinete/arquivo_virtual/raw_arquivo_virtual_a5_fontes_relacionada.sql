{{
    config(
        alias='fontes_relacionadas'        
    )
}}

SELECT
    safe_cast(ds_existLocalOriginais as string) as existencia_local_originais,
    safe_cast(ds_notasPublicacao as string) as notas_publicacao,
    safe_cast(ds_unidDescRelacionadas as string) as unidades_descricao_relacionadas,
    safe_cast(ds_outrosDetentores as string) as outros_detentores,
    safe_cast(ds_copiasNaInstituicao as string) as copias_na_instituicao,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at                  
FROM {{ source('brutos_arquivo_virtual_staging', 'a5_fontes_relacionada') }}
