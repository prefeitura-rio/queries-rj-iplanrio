{{
    config(
        alias='eai_gateway_incidents',
        schema='brutos_betterstack',
        materialized='table',
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by="id"
    )
}}

/*
Objetivo: Tratamentos básicos na tabela bruta de incidentes do BetterStack.
- Desaninhamento de campos JSON (attributes e relationships).
- Tipagem de dados e conversão de timestamps (mantidos em UTC).
- Filtro de campos ruidosos para otimizar a tabela final.
*/

WITH source AS (
    SELECT * FROM {{ source('brutos_betterstack_staging', 'eai_gateway_incidents') }}
),

processed AS (
    SELECT
        *,
        -- Transformando o dicionário stringivied do Python em JSON válido (aspas duplas, null, true, false)
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(attributes, "'", '"'), 'None', 'null'), 'True', 'true'), 'False', 'false'), '"{', '{') AS attributes_json,
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(relationships, "'", '"'), 'None', 'null'), 'True', 'true'), 'False', 'false'), '"{', '{') AS relationships_json
    FROM source
),

unnested AS (
    SELECT
        SAFE_CAST(id AS STRING) as id,
        -- Extraindo campos usando JSON_EXTRACT_SCALAR (agora robusto após a conversão)
        SAFE_CAST(JSON_EXTRACT_SCALAR(attributes_json, '$.name') AS STRING) as name,
        SAFE_CAST(JSON_EXTRACT_SCALAR(attributes_json, '$.status') AS STRING) as status,
        SAFE_CAST(JSON_EXTRACT_SCALAR(attributes_json, '$.cause') AS STRING) as cause,
        SAFE_CAST(JSON_EXTRACT_SCALAR(attributes_json, '$.url') AS STRING) as url,
        SAFE_CAST(JSON_EXTRACT_SCALAR(attributes_json, '$.team_name') AS STRING) as team_name,
        
        -- Timestamps
        SAFE_CAST(JSON_EXTRACT_SCALAR(attributes_json, '$.started_at') AS TIMESTAMP) as started_at,
        SAFE_CAST(JSON_EXTRACT_SCALAR(attributes_json, '$.resolved_at') AS TIMESTAMP) as resolved_at,
        SAFE_CAST(JSON_EXTRACT_SCALAR(attributes_json, '$.acknowledged_at') AS TIMESTAMP) as acknowledged_at,
        
        -- Extraindo response_code de dentro do response_options (que já deve ser um JSON válido após o processamento)
        SAFE_CAST(JSON_EXTRACT_SCALAR(attributes_json, '$.response_options.response_code') AS INT64) as response_code,
        
        -- Extraindo monitor_id das relationships
        SAFE_CAST(JSON_EXTRACT_SCALAR(relationships_json, '$.monitor.data.id') AS STRING) as monitor_id,
        
        -- Metadados de partição
        SAFE_CAST(ano_particao AS STRING) as ano_particao,
        SAFE_CAST(mes_particao AS STRING) as mes_particao,
        SAFE_CAST(data_particao AS DATE) as data_particao
    FROM processed
)

SELECT * FROM unnested