{{
    config(
        alias='dimensao_suporte'
    )
}}

SELECT
    safe_cast(id_conjuntoArquivo as int64) as id_conjuntoArquivo,
    safe_cast(id_genero as int64) as id_genero,
    safe_cast(id_especie as int64) as id_especie,
    safe_cast(id_capacidade as int64) as id_capacidade,
    safe_cast(id_tipoEscala as int64) as id_tipoEscala,
    safe_cast(id_formato as int64) as id_formato,
    safe_cast(qt_unidade as numeric) as qt_unidade,
    safe_cast(ds_escala as string) as ds_escala,
    safe_cast(obs as string) as obs,
    safe_cast(id_unidade as int64) as id_unidade,
    safe_cast(qt_capacidade as numeric) as qt_capacidade,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'dimensao_suporte') }}
