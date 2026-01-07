{{
    config(
        alias='eai_gateway_uptime_kpi',
        schema='brutos_betterstack',
        materialized='table'
    )
}}

/*
Objetivo: Calcular a disponibilidade mensal (Uptime %) do EAI Gateway.
- Baseado nos incidentes da tabela materializada 1.
- Timestamps convertidos para o horário de Brasília (America/Sao_Paulo).
- Fórmula: (Total minutos no mês - Minutos de indisponibilidade) / Total minutos no mês.
*/

WITH incidents AS (
    SELECT
        id,
        started_at,
        resolved_at,
        -- Conversão para o horário de Brasília conforme requisito
        DATETIME(started_at, 'America/Sao_Paulo') as started_at_br,
        DATETIME(resolved_at, 'America/Sao_Paulo') as resolved_at_br,
        -- Diferença em minutos (calculada em UTC para evitar problemas com fuso/DST)
        TIMESTAMP_DIFF(resolved_at, started_at, MINUTE) as downtime_minutes
    FROM {{ ref('raw_eai_gateway_incidents') }}
    WHERE status = 'Resolved' -- Apenas incidentes finalizados possuem tempo de indisponibilidade definido
),

monthly_downtime AS (
    SELECT
        FORMAT_DATETIME('%Y-%m', started_at_br) as mes,
        SUM(downtime_minutes) as total_downtime_minutes
    FROM incidents
    GROUP BY 1
),

month_stats AS (
    SELECT
        mes,
        total_downtime_minutes,
        -- Cálculo da quantidade total de minutos no mês
        EXTRACT(DAY FROM LAST_DAY(SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(mes, '-01')))) * 24 * 60 as total_minutes_in_month
    FROM monthly_downtime
)

SELECT
    mes,
    total_downtime_minutes,
    total_minutes_in_month,
    -- Cálculo de disponibilidade percentual
    ROUND(
        (1 - (SAFE_DIVIDE(total_downtime_minutes, total_minutes_in_month))) * 100, 
        3
    ) as disponibilidade_percentual
FROM month_stats
ORDER BY mes DESC
