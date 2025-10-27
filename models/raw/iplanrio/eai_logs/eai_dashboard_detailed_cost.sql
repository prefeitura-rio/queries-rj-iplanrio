{{
    config(
        alias="eai_dashboard_detailed_cost",
        schema="brutos_eai_logs",
        materialized="table",
    )
}}

WITH history AS (
  SELECT
    h.environment,
    h.last_update,
    h.user_id,
    w.nome,
    w.group as grupo,
    h.session_id,
    h.message_timestamp,
    DATE(message_timestamp) AS message_day,
    DATE_TRUNC(DATE(message_timestamp), MONTH) AS message_month,
    h.message_type,
    h.message_id,
    h.message_content,
    h.time_since_last_message_sec,
    h.model_name,
    h.prompt_tokens,
    h.candidates_tokens,
    h.total_tokens,
    h.tool_call_id,
    h.tool_name,
    h.tool_call_arguments_json,
    h.tool_return_payload,
  FROM {{ source('brutos_eai_logs', 'history') }} h
  JOIN {{ source('brutos_eai_logs', 'whitelist_beta') }} w
      ON h.user_id = w.user_id 
  WHERE message_timestamp IS NOT NULL 
    AND environment = 'prod'
),

custos AS (
  (SELECT 
      CAST(usage_start_time AS DATE) as dia,
      CASE 
        WHEN service.description IN ('BigQuery', 'Cloud Logging', 'Cloud Monitoring', 'Cloud SQL', 'Cloud Storage', 'Compute Engine', 'Kubernetes Engine', 'Networking', 'Geocoding API', 'Security Command Center') THEN "compartilhado"
        WHEN service.description IN ('Vertex AI', 'Gemini API', 'Cloud Speech API') THEN "exclusivo"
        ELSE "nao_categorizado" 
      END as tipo_custo,
      service.description as servico,
      SUM(cost) as custo_brl
    FROM {{ source('dados_rio_billing', 'gcp_billing_export_resource_v1_017DB4_593D94_BEEC50') }}
    WHERE project.id = "rj-superapp"
      AND usage_start_time >= '{{ var("CHATBOT_START_DATE") }}'
      -- Modificação: Pega os dados ANTES do dia da virada.
      AND usage_start_time < '{{ var("BILLING_CUTOVER_DATE") }} 17:00:00 UTC'
      AND _PARTITIONTIME >= DATE_SUB(DATE('{{ var("CHATBOT_START_DATE") }}'), INTERVAL 1 DAY)
      AND _PARTITIONTIME < DATE_ADD(DATE('{{ var("BILLING_CUTOVER_DATE") }}'), INTERVAL 1 DAY)
    GROUP BY 1,2,3
  )

  UNION ALL

  (SELECT 
      CAST(usage_start_time AS DATE) as dia,
      CASE 
        WHEN service.description IN ('BigQuery', 'Cloud Logging', 'Cloud Monitoring', 'Cloud SQL', 'Cloud Storage', 'Compute Engine', 'Kubernetes Engine', 'Networking', 'Geocoding API', 'Security Command Center') THEN "compartilhado"
        WHEN service.description IN ('Vertex AI', 'Gemini API', 'Cloud Speech API') THEN "exclusivo"
        ELSE "nao_categorizado" 
      END as tipo_custo,
      service.description as servico,
      SUM(cost) as custo_brl
    FROM {{ source('iplan_billing', 'gcp_billing_export_resource_v1_01F105_3E298A_65D256') }}
    WHERE project.id = "rj-superapp"
      -- Modificação: Pega os dados A PARTIR do dia da virada (inclusive).
      AND usage_start_time >= '{{ var("BILLING_CUTOVER_DATE") }} 17:00:00 UTC'
      AND _PARTITIONTIME >= DATE('{{ var("BILLING_CUTOVER_DATE") }}')
    GROUP BY 1,2,3
  )
),

cost_date as (
  SELECT
    CAST(MIN(dia) AS DATE) as data_inicial,
    CAST(MAX(dia) AS DATE) as data_final
  FROM custos
), 

historico_filtrado AS (
SELECT
    h.environment,
    h.user_id,
    h.grupo,
    h.session_id,
    h.message_day,
    h.message_type,
    h.message_id,
    h.model_name,
    h.tool_name
FROM history as h
WHERE h.message_day BETWEEN (SELECT data_inicial FROM cost_date) AND (SELECT data_final FROM cost_date)
),

user_per_day AS (
  SELECT
    message_day,
    COUNT(message_day) as user_messages,
    COUNT(DISTINCT session_id) as user_sessions,
  FROM historico_filtrado
  WHERE message_type = "user_message"
  GROUP BY 1
),


model_calls_per_day AS (
  SELECT
    message_day,
    COUNT(message_day) as model_calls,
    COUNT(DISTINCT session_id) as sessions,
  FROM historico_filtrado
  WHERE model_name IS NOT NULL
  GROUP BY 1
),


google_search_activated AS (
    SELECT
    message_day,
    COUNT(message_day) as model_calls,
    COUNT(DISTINCT session_id) as sessions,
  FROM historico_filtrado
  WHERE tool_name = 'google_search'
  GROUP BY 1
),

final_tb AS (
  SELECT
    c.dia,
    c.tipo_custo,
    c.servico,
    c.custo_brl,
    u.user_messages,
    u.user_sessions,
    CASE
      WHEN c.servico IN ('Vertex AI') THEN mc.model_calls
      ELSE CAST(NULL AS INT64) 
    END as model_calls_vertex,
    CASE
      WHEN c.servico IN ('Gemini API') THEN gs.model_calls
      ELSE CAST(NULL AS INT64) 
    END as model_calls_gemini,
    CASE
      WHEN c.servico IN ('Vertex AI') THEN mc.sessions
      ELSE CAST(NULL AS INT64) 
    END as sessions_vertex,
    CASE
      WHEN c.servico IN ('Gemini API') THEN gs.sessions
      ELSE CAST(NULL AS INT64) 
    END as sessions_gemini,
    
  FROM custos c
  LEFT JOIN model_calls_per_day mc
    on c.dia = mc.message_day
  LEFT JOIN google_search_activated gs
    on c.dia = gs.message_day
  LEFT JOIN user_per_day u
    on c.dia = u.message_day
)



SELECT
  dia,
  tipo_custo,
  servico,
  custo_brl,
  user_messages,
  user_sessions,
  model_calls_vertex,
  sessions_vertex,
  model_calls_gemini,
  sessions_gemini,
  CURRENT_TIMESTAMP() AS loaded_at
FROM final_tb
ORDER BY 1, 2,3
