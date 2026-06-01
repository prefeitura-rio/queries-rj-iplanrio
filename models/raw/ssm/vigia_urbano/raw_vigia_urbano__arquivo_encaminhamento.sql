{{
    config(
        alias="arquivo_encaminhamento",
        description="Tabela de arquivos de encaminhamento do sistema Vigia Urbano.",
        
    )
}}

SELECT
    safe_cast(arquivoEncaminhamentoID AS string) AS id_arquivo_encaminhamento,
    safe_cast(arquivo AS string) AS arquivo,
    safe_cast(encaminhamentoID AS int64) AS id_encaminhamento,
    safe.parse_datetime('%Y-%m-%d %H:%M:%E*S', _prefect_extracted_at) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'ArquivoEncaminhamento') }}