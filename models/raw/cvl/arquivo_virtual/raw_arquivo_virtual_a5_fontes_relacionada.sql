{{
    config(
        alias='a5_fontes_relacionada'        
    )
}}

SELECT
    safe_cast(ds_existLocalOriginais as string) as ds_existLocalOriginais,
    safe_cast(ds_notasPublicacao as string) as ds_notasPublicacao,
    safe_cast(ds_unidDescRelacionadas as string) as ds_unidDescRelacionadas,
    safe_cast(ds_outrosDetentores as string) as ds_outrosDetentores,
    safe_cast(ds_copiasNaInstituicao as string) as ds_copiasNaInstituicao,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at                  
FROM {{ source('brutos_arquivo_virtual_staging', 'a5_fontes_relacionada') }}
