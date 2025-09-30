{{
    config(
        alias='dimensao_suporte'
    )
}}

SELECT
    safe_cast(id_conjuntoArquivo as int64) as codigo_conjunto_arquivo,
    safe_cast(id_genero as int64) as codigo_genero,
    safe_cast(id_especie as int64) as codigo_especie,
    safe_cast(id_capacidade as int64) as codigo_capacidade,
    safe_cast(id_tipoEscala as int64) as codigo_tipo_escala,
    safe_cast(id_formato as int64) as codigo_formato,
    safe_cast(qt_unidade as numeric) as quantidade_unidade,
    safe_cast(ds_escala as string) as escala,
    safe_cast(obs as string) as observacoes,
    safe_cast(id_unidade as int64) as codigo_unidade,
    safe_cast(qt_capacidade as numeric) as quantidade_capacidade,
    _prefect_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at              
FROM {{ source('brutos_arquivo_virtual_staging', 'dimensao_suporte') }}
