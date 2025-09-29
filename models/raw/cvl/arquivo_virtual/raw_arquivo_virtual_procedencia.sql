{{
    config(
        alias='procedencia'
    )
}}

SELECT
    safe_cast(id_procedencia as int64) as id_procedencia,
    safe_cast(nr_geralEntradaProc as string) as nr_geralEntradaProc,
    safe_cast(nr_anoProc as string) as nr_anoProc,
    safe_cast(nm_procedencia as string) as nm_procedencia,
    safe_cast(id_formaEntrada as int64) as id_formaEntrada,
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'procedencia') }}
