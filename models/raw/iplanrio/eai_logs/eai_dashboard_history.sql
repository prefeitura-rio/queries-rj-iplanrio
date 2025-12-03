{{
    config(
        alias="eai_dashboard_history",
        schema="brutos_eai_logs",
        materialized="table",
    )
}}


WITH tb AS (
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

-- NOVO CTE: Calcula a categoria de cada usuário com base no histórico de atividade.
classificacao_usuarios AS (
  SELECT
    user_id,
    -- A lógica CASE determina a categoria. A ordem das condições é importante.
    CASE
      -- REGRA 1: Novo Usuário. A primeira interação do usuário ocorreu nos últimos 30 dias.
      WHEN MIN(DATE(message_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        THEN 'novo_usuario'
      
      -- REGRA 2: Usuário de Retorno. Ativo nos últimos 30 dias E antes de 60 dias, MAS inativo entre 30-60 dias atrás.
      WHEN 
        COUNTIF(DATE(message_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) > 0 -- Ativo nos últimos 30 dias
        AND COUNTIF(DATE(message_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)) = 0 -- Inativo entre 30-60 dias
        AND COUNTIF(DATE(message_timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)) > 0 -- Teve atividade antes disso
        THEN 'usuario_de_retorno'

      -- REGRA 3: Usuário Recorrente. Ativo nos últimos 30 dias E também ativo na janela de 30-60 dias atrás.
      WHEN 
        COUNTIF(DATE(message_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) > 0 -- Ativo nos últimos 30 dias
        AND COUNTIF(DATE(message_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)) > 0 -- Ativo também entre 30-60 dias
        THEN 'usuario_recorrente'

      -- Categoria extra para usuários que não se encaixam nas regras acima (ex: inativos)
      ELSE 'outro'
    END AS categoria_usuario
  FROM tb
  GROUP BY user_id -- Agrupamos para analisar o histórico completo de cada usuário.
),

marked AS (
  SELECT
    tb.*,
    EXISTS (
      SELECT 1
      FROM {{ source('brutos_eai_logs', 'feedback') }} f
      WHERE f.user_id = tb.user_id
        AND f.environment = 'production'
        AND f.feedback = tb.message_content
    ) AS feedback_match_exact
  FROM tb
),

numbered AS (
  SELECT m.*,
    CASE 
      WHEN m.message_type != 'tool_execution'
        THEN ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY message_timestamp)
    END AS msg_order
  FROM marked m
),

propagated AS (
  SELECT n.*,
    CASE 
      WHEN n.feedback_match_exact THEN TRUE
      WHEN MAX(CAST(feedback_match_exact AS INT64)) 
           OVER (
             PARTITION BY n.user_id 
             ORDER BY n.msg_order ASC
             ROWS BETWEEN 0 FOLLOWING AND 4 FOLLOWING
           ) = 1
      THEN TRUE
      ELSE FALSE
    END AS feedback_match
  FROM numbered n
)

-- Seleção Final: Juntamos os resultados da sua query com a nova tabela de classificação
SELECT 
  p.*, -- Seleciona todas as colunas da sua query original
  c.categoria_usuario, -- Adiciona a nova coluna de categoria
  CURRENT_TIMESTAMP() AS loaded_at
FROM propagated p
-- JUNÇÃO (JOIN): Conecta os dados da query com a classificação pelo user_id
JOIN classificacao_usuarios c ON p.user_id = c.user_id
WHERE message_day >= '{{ var("CHATBOT_START_DATE") }}'
ORDER BY user_id, message_timestamp