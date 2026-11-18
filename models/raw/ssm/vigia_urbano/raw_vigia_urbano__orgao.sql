{{
    config(
        alias="orgao",
        description="Tabela de Orgãos do sistema Vigia Urbano.",
        
    )
}}

SELECT
    safe_cast(orgaoID AS int64) AS id_orgao,
    safe_cast(TRIM(CAST(Nome AS STRING)) AS string) AS nome_orgao,
    safe_cast(codigo AS int64) AS codigo_orgao,
    safe_cast(subPrefeitura AS int64) AS id_subprefeitura,
    safe_cast(TRIM(CAST(subPrefJson AS STRING)) AS string) AS subprefeitura_json,
    safe_cast(_prefect_extracted_at as datetime) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'Orgao') }}
