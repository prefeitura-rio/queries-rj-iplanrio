{{
    config(
        alias='a7_controle_responsabilidade'
    )
}}

SELECT
    safe_cast(ds_datasDesc as string) as datas_descricao,
    safe_cast(ds_regrasConvencoes as string) as regras_convencoes,
    safe_cast(ds_notaArquivistica as string) as nota_arquivistica,
    safe_cast(ds_unidadeCustodiadora as string) as unidade_custodiadora,
    safe_cast(st_arquivoDigital as boolean) as status_arquivo_digital,
    safe_cast(ds_responsavelDesc as string) as responsavel_descricao,
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'a7_controle') }}
