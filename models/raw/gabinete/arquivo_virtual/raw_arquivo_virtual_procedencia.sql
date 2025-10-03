{{
    config(
        alias='procedencia'
    )
}}

SELECT
    safe_cast(id_procedencia as int64) as id_procedencia,
    safe_cast(nr_geralEntradaProc as string) as numero_geral_entrada,
    safe_cast(nr_anoProc as string) as ano_procedencia,
    safe_cast(nm_procedencia as string) as nome_procedencia,
    safe_cast(id_formaEntrada as int64) as id_forma_entrada,
    safe_cast(id_conjuntoArquivo as int64) as id_conjunto_arquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'procedencia') }}
