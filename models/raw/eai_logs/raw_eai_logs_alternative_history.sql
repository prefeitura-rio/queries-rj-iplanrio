WITH unpacked_logs AS (
  -- Passo 1: Extrair e desagrupar o array JSON em linhas individuais.
  SELECT
    project_id,
    CAST(last_update AS TIMESTAMP) as last_update,
    user_id,
    message
  FROM
    `rj-iplanrio.brutos_eai_logs_staging.history`,
    UNNEST(JSON_EXTRACT_ARRAY(messages)) AS message
  WHERE
    project_id = 'rj-superapp'
)
-- Passo 2: Parsear cada linha de JSON, extraindo e tipando cada campo.
SELECT
  -- 1. Metadados da Tabela Original
  project_id,
  last_update,
  user_id,

  -- 2. Identificadores Principais da Mensagem
  JSON_EXTRACT_SCALAR(message, '$.id') AS message_id,
  CAST(JSON_EXTRACT_SCALAR(message, '$.date') AS TIMESTAMP) AS message_timestamp,
  JSON_EXTRACT_SCALAR(message, '$.session_id') AS session_id,
  
  -- MODIFICAÇÃO: Unifica os tipos de mensagem de ferramenta
  CASE
    WHEN JSON_EXTRACT_SCALAR(message, '$.message_type') IN ('tool_call_message', 'tool_return_message')
    THEN 'tool_message'
    ELSE JSON_EXTRACT_SCALAR(message, '$.message_type')
  END AS message_type,
  
  JSON_EXTRACT_SCALAR(message, '$.step_id') AS step_id,
  
  -- 3. Conteúdo da Mensagem
  JSON_EXTRACT_SCALAR(message, '$.content') AS message_content,

  -- 4. Detalhes da Execução e Status
  COALESCE(JSON_EXTRACT_SCALAR(message, '$.is_err'), 'false') = 'true' AS is_error,
  JSON_EXTRACT_SCALAR(message, '$.status') AS status,
  CAST(JSON_EXTRACT_SCALAR(message, '$.time_since_last_message') AS FLOAT64) AS time_since_last_message_sec,
  
  -- 5. Detalhes do Modelo e Geração
  JSON_EXTRACT_SCALAR(message, '$.model_name') AS model_name,
  JSON_EXTRACT_SCALAR(message, '$.finish_reason') AS finish_reason,

  -- 6. Métricas de Custo e Uso ('usage_metadata')
  CAST(JSON_EXTRACT_SCALAR(message, '$.usage_metadata.prompt_token_count') AS INT64) AS prompt_tokens,
  CAST(JSON_EXTRACT_SCALAR(message, '$.usage_metadata.candidates_token_count') AS INT64) AS candidates_tokens,
  CAST(JSON_EXTRACT_SCALAR(message, '$.usage_metadata.total_token_count') AS INT64) AS total_tokens,
  
  -- 7. Dados de Ferramentas (Tool Call & Return)
  COALESCE(
    JSON_EXTRACT_SCALAR(message, '$.tool_call_id'),
    JSON_EXTRACT_SCALAR(message, '$.tool_call.tool_call_id')
  ) AS tool_call_id,

  COALESCE(
    JSON_EXTRACT_SCALAR(message, '$.name'),
    JSON_EXTRACT_SCALAR(message, '$.tool_call.name')
  ) AS tool_name,
  
  JSON_EXTRACT(message, '$.tool_call.arguments') AS tool_call_arguments_json,
  JSON_EXTRACT(message, '$.tool_return') AS tool_return_payload,
  JSON_EXTRACT_SCALAR(message, '$.stderr') AS tool_stderr
  
FROM unpacked_logs
ORDER BY user_id, message_timestamp;