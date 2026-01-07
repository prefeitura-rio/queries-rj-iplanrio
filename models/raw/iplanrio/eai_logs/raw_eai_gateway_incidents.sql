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

unnested AS (
    SELECT
        SAFE_CAST(id AS STRING) as id,
        -- Extraindo campos usando Regex (mais robusto para o formato stringified Python dict no BigQuery)
        REGEXP_EXTRACT(attributes, r"['\"]name['\"]\s*:\s*['\"]([^'\"]+)['\"]") as name,
        REGEXP_EXTRACT(attributes, r"['\"]status['\"]\s*:\s*['\"]([^'\"]+)['\"]") as status,
        REGEXP_EXTRACT(attributes, r"['\"]cause['\"]\s*:\s*['\"]([^'\"]+)['\"]") as cause,
        REGEXP_EXTRACT(attributes, r"['\"]url['\"]\s*:\s*['\"]([^'\"]+)['\"]") as url,
        REGEXP_EXTRACT(attributes, r"['\"]team_name['\"]\s*:\s*['\"]([^'\"]+)['\"]") as team_name,
        
        -- Timestamps (SAFE_CAST handles ISO8601 strings like '2026-01-02T20:23:09.291Z' naturally)
        SAFE_CAST(REGEXP_EXTRACT(attributes, r"['\"]started_at['\"]\s*:\s*['\"]([^'\"]+)['\"]") AS TIMESTAMP) as started_at,
        SAFE_CAST(REGEXP_EXTRACT(attributes, r"['\"]resolved_at['\"]\s*:\s*['\"]([^'\"]+)['\"]") AS TIMESTAMP) as resolved_at,
        SAFE_CAST(REGEXP_EXTRACT(attributes, r"['\"]acknowledged_at['\"]\s*:\s*['\"]([^'\"]+)['\"]") AS TIMESTAMP) as acknowledged_at,
        
        -- Extraindo response_code de dentro do response_options
        SAFE_CAST(REGEXP_EXTRACT(attributes, r"['\"]response_code['\"]\s*:\s*(\d+)") AS INT64) as response_code,
        
        -- Extraindo monitor_id das relationships
        REGEXP_EXTRACT(relationships, r"['\"]id['\"]\s*:\s*['\"]([^'\"]+)['\"]") as monitor_id,
        
        -- Metadados de partição
        SAFE_CAST(ano_particao AS STRING) as ano_particao,
        SAFE_CAST(mes_particao AS STRING) as mes_particao,
        SAFE_CAST(data_particao AS DATE) as data_particao
    FROM source
)

SELECT * FROM unnested