{{ config(
    alias='aca_religiao',
    materialized='table'
) }}

SELECT
    safe_cast(rlg_id AS INT64) AS identificador_religiao,
    safe_cast(rlg_nome AS STRING) AS nome_religiao,
    safe_cast(rlg_situacao AS INT64) AS situacao_religiao,
    --_prefect_extracted_at as loaded_at,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_gestao_escolar_staging', 'ACA_Religiao') }}
