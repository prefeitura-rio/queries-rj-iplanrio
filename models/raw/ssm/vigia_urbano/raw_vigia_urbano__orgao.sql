{{
    config(
        alias="orgao",
        description="Tabela de Orgãos do sistema Vigia Urbano.",
    )
}}

SELECT
    safe_cast(id_orgao AS int64) AS id_orgao,
    safe_cast(TRIM(nome_orgao) AS string) AS nome_orgao,
    safe_cast(codigo_orgao AS int64) AS codigo_orgao,
    safe_cast(id_subprefeitura AS int64) AS id_subprefeitura,
    safe_cast(TRIM(subprefeitura_json) AS string) AS subprefeitura_json,
    safe.parse_datetime('%Y-%m-%d %H:%M:%E*S', _prefect_extracted_at) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'Orgao') }}
