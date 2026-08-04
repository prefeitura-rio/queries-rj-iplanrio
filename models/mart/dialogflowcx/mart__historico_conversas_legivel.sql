{{
    config(
        alias='historico_conversas_legivel'
    )
}}

WITH historico_padrao AS (
  select
    h.conversation_name as conversa_completa_id,
    h.turn_position as conversa_completa_turn_position,
    h.conversation_name,
    h.turn_position,
    JSON_VALUE(JSON_EXTRACT(h.response, '$.queryResult.text')) as mensagem_cidadao,
    JSON_VALUE(JSON_EXTRACT(h.derived_data, '$.agentUtterances')) as resposta_bot,
    FORMAT_DATETIME("%d/%m/%Y às %H:%M", DATETIME(h.request_time, "America/Buenos_Aires")) as horario,
    JSON_EXTRACT(h.response, '$.queryResult.parameters') as parametros,
    h.request_time,
    h.response
  from `rj-chatbot-dev.dialogflowcx.historico_conversas` as h
  LEFT JOIN (
    SELECT
      conversation_name,
      turn_position,
      COUNT(*) AS contagem
    FROM rj-chatbot-dev.dialogflowcx.historico_conversas
    GROUP BY conversation_name, turn_position
    HAVING COUNT(*) > 1
  ) AS bc
    ON h.conversation_name = bc.conversation_name
  WHERE bc.conversation_name IS NULL -- Remove conversations with more than one conversation where turn_position = 1, probably bot cases
),

historico_macrofluxo AS (
  SELECT
    conversation_name as conversa_completa_id,
    turn_position as conversa_completa_turn_position,
    new_conversation_id as conversation_name,
    new_turn as turn_position,
    mensagem_cidadao,
    resposta_bot,
    horario,
    parametros,
    request_time,
    response
  FROM {{ ref('mart__historico_conversas_macrofluxos') }}
)

-- Combine rows where conversation_name does not exist in the other table
(
  SELECT hp.*
  FROM historico_padrao hp
  LEFT JOIN historico_macrofluxo hm
    ON hp.conversation_name = hm.conversation_name
  WHERE hm.conversation_name IS NULL

  UNION ALL

  SELECT hm.*
  FROM historico_macrofluxo hm
  LEFT JOIN historico_padrao hp
    ON hm.conversation_name = hp.conversation_name
  WHERE hp.conversation_name IS NULL
)

ORDER BY conversation_name, request_time ASC